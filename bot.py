from aiohttp import web
from datetime import datetime, timezone, timedelta
from icalendar import Calendar, Event, FreeBusy
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from rich import print
from rich.pretty import pprint
from pathlib import Path
import argparse
import asyncio
import discord
import memcache
import pathlib
import requests
import sys
import yaml


# Define file paths
PROJECT_ROOT = pathlib.Path(__file__).parent.resolve()
CONFIG_FILE_PATH = Path(PROJECT_ROOT) / "config.yaml"
FRONTEND_ROOT_PATH = Path(PROJECT_ROOT) / "frontend"
FRONTEND_STATIC_PATH = Path(FRONTEND_ROOT_PATH) / "static"


# Guild Scheduled Event Object Fields that indicate a meaningful change to the event.
#
# Discord API output includes fields that may change
# but not impact ICS output, e.g. creator.accent_color.
#
# "subscribed_users" is not included in this list
# because it does not affect VEVENT values,
# and is only used in custom feeds to filter events based on user ID.
#
# https://docs.discord.com/developers/resources/guild-scheduled-event
MEANINGFUL_FIELDS = [
    "name",
    "description",
    "scheduled_start_time",
    "scheduled_end_time",
    "entity_metadata",  # .location
]


# Build ArgumentParser https://docs.python.org/3/library/argparse.html
arg_parser = argparse.ArgumentParser()
arg_parser.add_argument(
    "--debug",
    action="store_true",
    help="Print extra values, including Discord Bot Token, and generate static .ics files",
)
args = arg_parser.parse_args()


config = {}
with CONFIG_FILE_PATH.open() as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(
            f"[red]Error:[/red] Could not read config file at [yellow]{CONFIG_FILE_PATH}[/yellow]!"
        )
        print(exc)
        sys.exit(1)


# Create MongoDB connection
mongo_client = MongoClient(config["mongodb"]["url"], retryWrites=True)
mongo_db = mongo_client.get_default_database()
mongo_collection = mongo_db.events


# Create Memcached client
memcache_client = memcache.Client(
    [config["memcache"]["url"]], debug=(1 if args.debug else 0)
)


def memcache_key_for_guild_events(guild_id):
    return f"{config['memcache']['key_prefix']}_guild_events_{guild_id}"


def memcache_key_for_guild_info(guild_id):
    return f"{config['memcache']['key_prefix']}_guild_info_{guild_id}"


def memcache_key_for_suggested_feeds():
    return f"{config['memcache']['key_prefix']}_suggested_feeds"


def log_guild_info(guild_info):
    if args.debug:
        print(f"Guild Info for [yellow]{guild_info.get('id', 'Unknown')}[/yellow]:")
        pprint(guild_info)
    else:
        print(
            f"Guild name for [yellow]{guild_info.get('id', 'Unknown')}[/yellow]: [green]{guild_info.get('name', 'Unknown')}[/green]"
        )


def discord_api_http_request(url):
    # discord.py internal architecture collides with the fact
    # Discoverable guilds allow fetching scheduled event data
    # without the bot being present on the server.
    # https://deepwiki.com/Rapptz/discord.py/7.3-scheduled-events
    #
    # So this API call:
    # `discord_client.http.get_scheduled_events(guild_id, with_user_count=True)`
    # stalls for MULTIPLE SECONDS,
    # and it's MUCH FASTER to bypass discord.py entirely
    headers = {"Authorization": f"Bot {config['discord']['token']}"}
    return requests.get(
        url, headers=headers, timeout=config["discord"]["http_request_timeout_seconds"]
    )


def get_guild_info(guild_id):
    memcache_key = memcache_key_for_guild_info(guild_id)
    cached_response = memcache_client.get(memcache_key)
    if cached_response:
        print(
            f"Using cached response for info of guild ID: [yellow]{guild_id}[/yellow]"
        )
        log_guild_info(cached_response)
        return cached_response

    url = f"https://discord.com/api/v10/guilds/{guild_id}/preview"
    response = discord_api_http_request(url)
    if response.status_code == requests.codes.ok:  # 200
        response_json = response.json()
        log_guild_info(response_json)
        memcache_client.set(
            memcache_key,
            response_json,
            time=config["memcache"]["guild_info_ttl_seconds"],
        )
        return response_json

    print(
        f"[red]Error:[/red] Failed to fetch Discord guild info for guild ID: [yellow]{guild_id}[/yellow]: {response.content}"
    )
    return None


def log_events(events, guild_id):
    print(f"Events for guild ID: [yellow]{guild_id}[/yellow]:")
    if args.debug:
        pprint(events)
    else:
        if len(events) == 0:
            print("No events found")
        for ev in events:
            human_readable_datetime = datetime.fromisoformat(
                ev["scheduled_start_time"]
            ).strftime("%Y-%m-%d %H:%M:%S")
            print(f" - {ev['id']} @ {human_readable_datetime}: {ev['name']}")


def retrieve_subscribed_users_for_event(guild_id, event_id):
    # This endpoint works for both Discoverable and Invite Only servers
    url = f"https://discord.com/api/v10/guilds/{guild_id}/scheduled-events/{event_id}/users"
    response = discord_api_http_request(url)
    if response.status_code == requests.codes.ok:  # 200
        # This implementation will only retrieve the first 100 subscribed users.
        # To retrieve more than 100 subscribed users,
        # we need to implement pagination using the "after" query parameter.
        # I cannot reproduce a scenario where an event has more than 100 subscribed users though.
        # For example, Marvel Rivals server (1193841000108531764)
        # has more than 4 million members but only 1-2 users subscribe to events.
        # World of Warcraft server (113103747126747136) has 150k members,
        # but less than 40 subscribed to Midnight launch event.
        return response.json()

    print(
        f"[red]Error:[/red] Failed to fetch subscribed users for event ID: [yellow]{event_id}[/yellow]: {response.content}"
    )
    return None


def retrieve_memcached_current_events_for_guild(guild_id):
    memcache_key = memcache_key_for_guild_events(guild_id)
    cached_response = memcache_client.get(memcache_key)
    if cached_response:
        print(
            f"Using cached response for events of guild ID: [yellow]{guild_id}[/yellow]"
        )
        return cached_response

    url = f"https://discord.com/api/v10/guilds/{guild_id}/scheduled-events?with_user_count=true"
    response = discord_api_http_request(url)
    if response.status_code == requests.codes.ok:  # 200
        response_json = response.json()

        for event in response_json:
            subscribed_users = retrieve_subscribed_users_for_event(
                guild_id, event["id"]
            )
            if subscribed_users is not None:
                event["subscribed_users"] = subscribed_users

        memcache_client.set(
            memcache_key, response_json, time=config["memcache"]["events_ttl_seconds"]
        )
        return response_json

    print(
        f"[red]Error:[/red] Failed to fetch Discord events for guild ID [yellow]{guild_id}[/yellow]: {response.content}"
    )
    return None


def upsert_event(event_data: dict):
    try:
        existing = mongo_collection.find_one({"id": event_data["id"]})

        # Check if any of the meaningful fields changed
        data_changed = existing is None or any(
            event_data.get(field) != existing.get(field) for field in MEANINGFUL_FIELDS
        )

        event_update = {
            "$set": {
                **event_data,
                "icalcord_last_seen_time": datetime.now(timezone.utc),
                # Convert ISO 8601 strings to datetime objects
                "icalcord_scheduled_start_time": datetime.fromisoformat(
                    event_data["scheduled_start_time"]
                ),
            },
            "$setOnInsert": {"icalcord_discovered_time": datetime.now(timezone.utc)},
        }

        # scheduled_end_time is only available for events with 'entity_type': 3,
        # aka EXTERNAL,
        # aka "Where is your event?" == "Somewhere Else. Text channel, external link or in-person location."
        if event_data["scheduled_end_time"]:
            event_update["$set"]["icalcord_scheduled_end_time"] = (
                datetime.fromisoformat(event_data["scheduled_end_time"])
            )

        # Only set updated time if something meaningful changed
        if data_changed:
            print(
                f"Meaningful update for event [magenta]{event_data['name']}[/magenta]"
            )
            event_update["$set"]["icalcord_updated_time"] = datetime.now(timezone.utc)

        result = mongo_collection.update_one(
            {"id": event_data["id"]},
            event_update,
            upsert=True,
        )

        if result.upserted_id:
            print(
                f"Inserted new event with _id: [yellow]{result.upserted_id}[/yellow] ([magenta]{event_data['name']}[/magenta])"
            )
        elif result.modified_count:
            print(f"Updated existing event ([magenta]{event_data['name']}[/magenta])")
        else:
            print("No changes needed")

    except PyMongoError as e:
        print("[red]Database error:[/red]", e)
        raise


def generate_ics_feed(guild_id):
    print(f"Generating .ics feed for guild ID: [yellow]{guild_id}[/yellow]")
    ics_path = Path(FRONTEND_STATIC_PATH) / f"{guild_id}.ics"

    guild_name = f"{guild_id}"
    guild_info = get_guild_info(guild_id)
    if guild_info:
        guild_name = guild_info.get("name", guild_id)
    else:
        print("[red]Warning:[/red] guild_info missing when generating ICS feed")

    events = mongo_collection.find({"guild_id": guild_id})

    # The icalendar library https://icalendar.readthedocs.io/en/stable/index.html
    # is fairly low level and can generate output that is not spec-compliant,
    # make sure to validate edge cases with https://icalendar.org/validator.html
    ics = Calendar()
    ics.prodid = "-//icalcord.retromultiplayer.com//iCalCord//EN"
    ics.version = "2.0"
    ics.calendar_name = f"{guild_name} Events"

    for event in events:
        ics_event = Event()
        ics_event.uid = f"{event['id']}@icalcord"
        ics_event.summary = event["name"]
        ics_event.stamp = event.get("icalcord_updated_time")

        # I'm not gonna throw away old event data (right now)
        if event.get("icalcord_scheduled_start_time"):
            ics_event.start = event["icalcord_scheduled_start_time"]
        elif isinstance(event["scheduled_start_time"], datetime):
            ics_event.start = event["scheduled_start_time"]
        else:
            ics_event.start = datetime.fromisoformat(event["scheduled_start_time"])

        if event.get("icalcord_scheduled_end_time"):
            ics_event.end = event["icalcord_scheduled_end_time"]
        elif isinstance(event["scheduled_end_time"], datetime):
            ics_event.end = event["scheduled_end_time"]
        elif event["scheduled_end_time"]:
            ics_event.end = datetime.fromisoformat(event["scheduled_end_time"])
        # Unless 'entity_type': 3,
        # we don't get event end time nor duration from Discord API.
        # RFC 5545 does not _require_ VEVENT to have DTEND or DURATION.
        # Most popular caledar apps default to 1 hour for new events.

        if event.get("description"):
            ics_event.description = event["description"]

        if event.get("entity_metadata") and event["entity_metadata"].get("location"):
            ics_event.location = event["entity_metadata"]["location"]

        ics.add_component(ics_event)

    if events.retrieved == 0:
        print(
            f"No events found for guild ID: [yellow]{guild_id}[/yellow], inserting VFREEBUSY component for spec compliance"
        )
        # Insert a VFREEBUSY component for spec compliance
        # when we don't have any events to insert at the time of feed generation.
        # This ensures that calendar clients will recognize the feed as valid
        # and continue to check for updates,
        # rather than discarding it as an invalid source.
        vfreebusy = FreeBusy()
        vfreebusy.uid = f"vfreebusy-{guild_id}@icalcord"
        # Arbitrary fixed date,
        # not too far in the past to avoid client rejections,
        # not too far in the future to avoid confusion
        vfreebusy.stamp = datetime(2026, 3, 1, tzinfo=timezone.utc)
        ics.add_component(vfreebusy)

    ics_feed = ics.to_ical()

    if args.debug:
        with ics_path.open("wb") as ics_file:
            ics_file.write(ics_feed)
            print(f"Saved static [green]{ics_path}[/green] file")

    return ics_feed


# Set up Discord API client
discord_intents = discord.Intents.default()
discord_client = discord.Client(intents=discord_intents)


def log_http_request(request):
    # Production instance runs behind reverse proxy,
    # so request.remote returns 127.0.0.1 there.
    # X-Forwarded-For header is set to actual client IP by proxy.
    client_ip = request.headers.get("X-Forwarded-For", request.remote)

    print(f"[blue]{client_ip}[/blue] HTTP requested: [green]{request.rel_url}[/green]")


async def frontend_index(request):
    log_http_request(request)

    return web.FileResponse(Path(FRONTEND_ROOT_PATH) / "index.html")


async def endpoint_handler_ics_feed_generator(request):
    log_http_request(request)

    guild_id = request.match_info["guild_id"]

    # Basic validation to ensure guild_id is a plausible Discord Snowflake.
    if not guild_id.isdigit() or not (16 <= len(guild_id) <= 20):
        return web.json_response({"error": "Invalid guild_id format"}, status=400)

    await fetch_and_store_events_for_guild(guild_id)
    ics_feed = generate_ics_feed(guild_id)
    return web.Response(body=ics_feed, content_type="text/calendar", charset="utf-8")


async def endpoint_handler_suggested_feeds(request):
    log_http_request(request)

    memcache_key = memcache_key_for_suggested_feeds()
    cached_output = memcache_client.get(memcache_key)
    if cached_output is not None:
        print("Responding with cached suggested feeds")
        return web.json_response(cached_output)

    print("Generating fresh suggested feeds")

    one_month_ago = datetime.now(timezone.utc) - timedelta(days=30)

    query_filter = {
        # don't suggest servers that have no recent events,
        # that would result in empty feeds
        "icalcord_scheduled_start_time": {"$gte": one_month_ago}
    }

    query_projection = {field: 1 for field in MEANINGFUL_FIELDS} | {
        "guild_id": 1,
        "_id": 0,  # exclude _id because type ObjectId is not JSON serializable
    }

    recent_events = list(mongo_collection.find(query_filter, query_projection))

    unique_guild_ids = list(set(map(lambda ev: ev["guild_id"], recent_events)))

    def format_guild_info(guild_id):
        guild_info = get_guild_info(guild_id)
        guild_info_keys = ["id", "name"]
        guild_info_trimmed = {k: guild_info[k] for k in guild_info_keys}

        guild_events = filter(lambda ev: ev["guild_id"] == guild_id, recent_events)
        guild_info_trimmed["scheduled_events"] = list(guild_events)

        return guild_info_trimmed

    guild_info = map(format_guild_info, unique_guild_ids)

    output = list(guild_info)

    # I could implement a more sophisticated value generation here, but:
    # - i want to shift processing to client machines as much as possible
    # - this process doesn't know external root URL, unlike JavaScript on the frontend

    memcache_client.set(
        memcache_key,
        output,
        time=config["memcache"]["suggested_feeds_ttl_seconds"],
    )

    return web.json_response(output)


async def start_http_server():
    app = web.Application()

    # Generate a feed for a specific Guild ID
    app.router.add_get("/feed/{guild_id}.ics", endpoint_handler_ics_feed_generator)

    # Query MongoDB for recent events,
    # for frontend to suggest feeds to subscribe to.
    app.router.add_get("/suggested.json", endpoint_handler_suggested_feeds)

    # Root index.html
    app.router.add_get("/", frontend_index)

    # Serve entire /frontend/static directory
    app.router.add_static("/static/", path=FRONTEND_STATIC_PATH)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, config["frontend"]["ip"], config["frontend"]["port"])
    await site.start()


async def fetch_and_store_events_for_guild(guild_id):
    print(f"Fetching events for guild ID: [yellow]{guild_id}[/yellow]")

    # The bot doesn't need to be present on server to fetch events
    # if the server is "Discoverable" (1000+ members) and events are "Public" (default setting).
    # https://docs.discord.com/developers/resources/guild-scheduled-event#list-scheduled-events-for-guild
    discoverable_events_json = retrieve_memcached_current_events_for_guild(guild_id)
    log_events(discoverable_events_json, guild_id)
    if discoverable_events_json is not None:
        for event_json in discoverable_events_json:
            upsert_event(event_json)
        return

    print(
        f"Attempting to fetch events using discord.py library for guild ID: [yellow]{guild_id}[/yellow]"
    )
    memcache_key = memcache_key_for_guild_events(guild_id)
    cached_response = memcache_client.get(memcache_key)
    if cached_response is not None:
        print(
            f"Using cached discord.py response for guild ID: [yellow]{guild_id}[/yellow]"
        )
        log_events(cached_response, guild_id)
        return

    guild = discord_client.get_guild(guild_id)
    if guild is None:
        print(
            f"[red]Error:[/red] Guild [yellow]{guild_id}[/yellow] not found (bot not invited?)"
        )
        return
    events = await guild.fetch_scheduled_events(with_counts=True)
    if not events:
        print("No scheduled events found.")
    else:
        print(f"Found [green]{len(events)}[/green] scheduled events:")

        for event in events:
            subscribed_users = guild.user(
                retrieve_subscribed_users_for_event(guild_id, event["id"])
            )
            if subscribed_users is not None:
                event["subscribed_users"] = subscribed_users

        log_events(events, guild_id)
    memcache_client.set(
        memcache_key, events, time=config["memcache"]["value_ttl_seconds"]
    )

    # TODO: Delete upcoming events that are absent from API response?
    # Is there a more reliable way to detect deleted events than relying on API response consistency?


@discord_client.event
async def on_ready():
    print(f"Logged into Discord as [yellow]{discord_client.user}[/yellow]")
    await start_http_server()
    print(
        f"Started HTTP Server on {config['frontend']['ip']}:{config['frontend']['port']}"
    )


# TODO: implement a slash command to respond with .ics feed URL for the server, e.g. /icalcord
@discord_client.event
async def on_message(message):
    if message.author == discord_client.user:
        return

    if message.content.startswith("$hello"):
        await message.channel.send("Hello!")


async def main():
    await discord_client.start(config["discord"]["token"])


if __name__ == "__main__":
    asyncio.run(main())
