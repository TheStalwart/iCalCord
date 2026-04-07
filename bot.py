import argparse
import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import discord
import memcache
import requests
import sentry_sdk
import yaml
from aiohttp import web
from icalendar import Calendar, Event, FreeBusy
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from rich import print as rprint
from rich.pretty import pprint

# Define file paths
PROJECT_ROOT = Path(__file__).parent.resolve()
CONFIG_FILE_PATH = Path(PROJECT_ROOT) / "config.yaml"
FRONTEND_ROOT_PATH = Path(PROJECT_ROOT) / "frontend"
FRONTEND_STATIC_PATH = Path(FRONTEND_ROOT_PATH) / "static"

# Guild Object Fields worth returning to frontend
GUILD_MEANINGFUL_FIELDS = ["id", "name", "description"]

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
EVENT_MEANINGFUL_FIELDS = [
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
    help="Print extra values and generate static ics/json files",
)
args = arg_parser.parse_args()


config = {}
with CONFIG_FILE_PATH.open() as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        rprint(
            f"[red]Error:[/red] Could not read config file"
            f" at [yellow]{CONFIG_FILE_PATH}[/yellow]!",
        )
        rprint(exc)
        sys.exit(1)
config["log"]["timezone_zoneinfo"] = ZoneInfo(config["log"]["timezone"])


# Initialize Sentry SDK.
# Silently ignore failure when debugging.
def initialize_sentry_sdk():
    sentry_config = config.get("sentry")

    if args.debug and not sentry_config:
        return

    sentry_dsn = sentry_config.get("dsn")

    if args.debug and not sentry_dsn:
        return

    try:
        sentry_sdk.init(
            dsn=sentry_dsn,
            # Set traces_sample_rate to 1.0 to capture 100%
            # of transactions for tracing.
            traces_sample_rate=1.0,
            # Set profiles_sample_rate to 1.0 to profile 100%
            # of sampled transactions.
            # We recommend adjusting this value in production.
            profiles_sample_rate=1.0,
        )
    except sentry_sdk.utils.BadDsn as exc:
        if not args.debug:
            log_sentry_init_failure(exc)


initialize_sentry_sdk()

# Create MongoDB connection
mongo_client = MongoClient(config["mongodb"]["url"], retryWrites=True)
mongo_db = mongo_client.get_default_database()
mongo_collection = mongo_db.events


# Create Memcached client
memcache_client = memcache.Client(
    [config["memcache"]["url"]],
    debug=(1 if args.debug else 0),
)


def memcache_key_for_guild_events(guild_id):
    return f"{config['memcache']['key_prefix']}_guild_events_{guild_id}"


def memcache_key_for_guild_info(guild_id):
    return f"{config['memcache']['key_prefix']}_guild_info_{guild_id}"


def memcache_key_for_suggested_feeds():
    return f"{config['memcache']['key_prefix']}_suggested_feeds"


def log_guild_info(guild_info):
    guild_id = guild_info.get("id", "Unknown")
    guild_name = guild_info.get("name", "Unknown")

    rprint(
        f"Guild name for [yellow]{guild_id}[/yellow]: [green]{guild_name}[/green]",
    )


def discord_api_http_request(url):
    # discord.py internal intent-based event-listening architecture
    # collides with the fact
    # Discoverable guilds allow fetching scheduled event data
    # without the bot being present on the server.
    # https://deepwiki.com/Rapptz/discord.py/7.3-scheduled-events
    #
    # Also, these kinds of direct API calls:
    # ` discord_client.http.get_scheduled_events(guild_id, with_user_count=True)
    # as well as using internal HTTP client with arbitrary URL:
    # ` data = await discord_client.http.request(
    # `     discord.http.Route("GET", f"/guilds/{guild_id}/scheduled-events"),
    # ` )
    # stall for MULTIPLE SECONDS,
    # and it's MUCH FASTER to bypass discord.py entirely.
    # I also don't like the fact
    # discord.py doesn't expose original JSON response,
    # and i want to keep a copy of scheduled event JSON in the database.
    headers = {"Authorization": f"Bot {config['discord']['token']}"}
    return requests.get(
        url,
        headers=headers,
        timeout=config["discord"]["http_request_timeout_seconds"],
    )


# Basic validation of a Discord Snowflake.
# https://docs.discord.com/developers/reference#snowflakes
# https://medium.com/netcord/discord-snowflake-explained-id-generation-process-a468be00a570
# https://discordutils.com/snowflake-decoder
def is_valid_discord_snowflake(snowflake):
    if snowflake is None:
        return False

    min_length = 16  # realistically, 17 should be the shortest ever
    max_length = 20  # expected maximum length of a 64bit integer
    return snowflake.isdigit() and (min_length <= len(snowflake) <= max_length)


def get_guild_info(guild_id):
    # discord.py keeps cached values for guilds the bot is invited to.
    # get_* functions shouldn't make any API requests when called,
    # values are being updated by receiving on_guild_update event via WebSocket
    client_cached_info = discord_client.get_guild(int(guild_id))
    if client_cached_info and client_cached_info.name:
        return {"id": guild_id, "name": client_cached_info.name}

    memcache_key = memcache_key_for_guild_info(guild_id)
    cached_response = memcache_client.get(memcache_key)
    if cached_response:
        rprint(
            f"Using cached response for info of guild ID: [yellow]{guild_id}[/yellow]",
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

    rprint(
        f"[red]Error:[/red] Failed to fetch Discord guild info"
        f" for guild ID: [yellow]{guild_id}[/yellow]: {response.content}",
    )
    return None


def log_sentry_init_failure(error):
    rprint("[red]Warning:[/red] Sentry initialization failed!")
    pprint(error)


def log_events(events):
    if len(events) == 0:
        rprint("No events found")
        return

    rprint("Events:")
    for ev in events:
        human_readable_datetime = (
            datetime.fromisoformat(
                ev["scheduled_start_time"],
            )
            .astimezone(config["log"]["timezone_zoneinfo"])
            .strftime(config["log"]["timestamp_format"])
        )
        rprint(f" - {ev['id']} @ {human_readable_datetime}: {ev['name']}")


def retrieve_subscribed_users_for_event(guild_id, event_id):
    # This endpoint works for both Discoverable and Invite Only servers
    url = f"https://discord.com/api/v10/guilds/{guild_id}/scheduled-events/{event_id}/users"
    response = discord_api_http_request(url)
    if response.status_code == requests.codes.ok:  # 200
        # This implementation will only retrieve the first 100 subscribed users.
        # To retrieve more than 100 subscribed users,
        # we need to implement pagination using the "after" query parameter.
        # I cannot reproduce a scenario
        # where an event has more than 100 subscribed users though.
        # For example, Marvel Rivals server (1193841000108531764)
        # has more than 4 million members but only 1-2 users subscribe to events.
        # World of Warcraft server (113103747126747136) has 150k members,
        # but less than 40 subscribed to Midnight launch event.
        return response.json()

    rprint(
        f"[red]Error:[/red] Failed to fetch subscribed users"
        f" for event ID: [yellow]{event_id}[/yellow]: {response.content}",
    )
    return None


def retrieve_memcached_current_events_for_guild(guild_id):
    memcache_key = memcache_key_for_guild_events(guild_id)
    cached_response = memcache_client.get(memcache_key)
    if cached_response:
        rprint(
            f"Using cached response"
            f" for events of guild ID: [yellow]{guild_id}[/yellow]",
        )
        return cached_response

    url = f"https://discord.com/api/v10/guilds/{guild_id}/scheduled-events?with_user_count=true"
    response = discord_api_http_request(url)
    if response.status_code == requests.codes.ok:  # 200
        response_json = response.json()

        # Sentry.io flagged this as "Consecutive HTTP".
        # This is, in fact, a bad idea.
        # "AI" suggested to group the calls, but that idea is even worse
        # due to Discord API request limits.
        # It's unusual for Discord servers to have lots of scheduled events,
        # but as of March 2026, one such server is
        # 229259188856029184: Eurovision Song Contest.
        # There are many ways to solve this,
        # i could keep track of the last update of subscribed_users in DB,
        # and update N oldest entries on every feed request.
        # Another way would be to only refresh subscribed_users
        # if someone requested a custom feed in the last N days.
        for event in response_json:
            subscribed_users = retrieve_subscribed_users_for_event(
                guild_id,
                event["id"],
            )
            if subscribed_users is not None:
                event["subscribed_users"] = subscribed_users

        memcache_client.set(
            memcache_key,
            response_json,
            time=config["memcache"]["events_ttl_seconds"],
        )
        return response_json

    rprint(
        f"[red]Error:[/red] Failed to fetch Discord events"
        f" for guild ID [yellow]{guild_id}[/yellow]: {response.content}",
    )
    return None


def upsert_event(event_data: dict):
    try:
        existing = mongo_collection.find_one({"id": event_data["id"]})

        # Check if any of the meaningful fields changed
        data_changed = existing is None or any(
            event_data.get(field) != existing.get(field)
            for field in EVENT_MEANINGFUL_FIELDS
        )

        event_update = {
            "$set": {
                **event_data,
                "icalcord_last_seen_time": datetime.now(timezone.utc),
                # Convert ISO 8601 strings to datetime objects
                "icalcord_scheduled_start_time": datetime.fromisoformat(
                    event_data["scheduled_start_time"],
                ),
            },
            "$setOnInsert": {"icalcord_discovered_time": datetime.now(timezone.utc)},
        }

        # scheduled_end_time is only available for events with 'entity_type': 3,
        # aka EXTERNAL,
        # aka "Where is your event?"
        #        == "Somewhere Else. Text channel, external link or in-person location."
        if event_data["scheduled_end_time"]:
            event_update["$set"]["icalcord_scheduled_end_time"] = (
                datetime.fromisoformat(event_data["scheduled_end_time"])
            )

        # Only set updated time if something meaningful changed
        if data_changed:
            rprint(
                f"Meaningful update for event [magenta]{event_data['name']}[/magenta]",
            )
            event_update["$set"]["icalcord_updated_time"] = datetime.now(timezone.utc)

        result = mongo_collection.update_one(
            {"id": event_data["id"]},
            event_update,
            upsert=True,
        )

        if result.upserted_id:
            rprint(
                f"Inserted new event with _id: [yellow]{result.upserted_id}[/yellow]"
                f" ([magenta]{event_data['name']}[/magenta])",
            )
        elif result.modified_count:
            action_executed = "Updated" if data_changed else "Verified"
            rprint(
                f"{action_executed} upcoming event"
                f" ([magenta]{event_data['name']}[/magenta])",
            )
        else:
            # This should never happen,
            # as we always update at least the `icalcord_last_seen_time` value
            rprint("[red]Warning:[/red] No changes made!")

    except PyMongoError as e:
        rprint("[red]Database error:[/red]", e)
        raise


def generate_ics_vevent(event):
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
    # Most popular calendar apps default to 1 hour for new events.

    if event.get("description"):
        ics_event.description = event["description"]

    if event.get("entity_metadata") and event["entity_metadata"].get("location"):
        ics_event.location = event["entity_metadata"]["location"]

    return ics_event


def generate_ics_calendar(guild_info):
    guild_id = f"{guild_info['id']}"
    guild_name = guild_info.get("name", guild_id)

    rprint(
        f"Generating .ics feed for guild ID:"
        f" [yellow]{guild_id}[/yellow] ([green]{guild_name}[/green])",
    )

    ics_path = Path(FRONTEND_STATIC_PATH) / f"{guild_id}.ics"

    events = mongo_collection.find({"guild_id": guild_id})

    # The icalendar library https://icalendar.readthedocs.io/en/stable/index.html
    # is fairly low level and can generate output that is not spec-compliant,
    # make sure to validate edge cases with https://icalendar.org/validator.html
    ics = Calendar()
    ics.prodid = "-//icalcord.retromultiplayer.com//iCalCord//EN"
    ics.version = "2.0"
    ics.calendar_name = f"{guild_name} Events"

    ics.subcomponents = list(map(generate_ics_vevent, events))

    if events.retrieved == 0:
        rprint(
            f"No events found for guild ID: [yellow]{guild_id}[/yellow],"
            f" inserting VFREEBUSY component for spec compliance",
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

    if args.debug:
        with ics_path.open("wb") as ics_file:
            ics_feed = ics.to_ical()
            ics_file.write(ics_feed)
            rprint(f"Saved static [green]{ics_path}[/green] file")

    return ics


# Set up Discord API client
discord_intents = discord.Intents(guilds=True, guild_scheduled_events=True)
discord_client = discord.Client(intents=discord_intents)


def log_http_request(request):
    # Production instance runs behind reverse proxy,
    # so request.remote returns 127.0.0.1 there.
    # X-Forwarded-For header is set to actual client IP by proxy.
    client_ip = request.headers.get("X-Forwarded-For", request.remote)

    timestamp_string = datetime.now(
        tz=config["log"]["timezone_zoneinfo"],
    ).strftime(config["log"]["timestamp_format"])

    rprint(
        f"{timestamp_string} [blue]{client_ip}[/blue]"
        f" HTTP requested: [green]{request.rel_url}[/green]",
    )


async def frontend_index(request):
    log_http_request(request)

    return web.FileResponse(Path(FRONTEND_ROOT_PATH) / "index.html")


async def endpoint_handler_preview(request):
    log_http_request(request)

    snowflake_or_invite_code = request.match_info["snowflake_or_invite_code"]
    guild_id = None
    guild_info = None

    if is_valid_discord_snowflake(snowflake_or_invite_code):
        guild_id = snowflake_or_invite_code
    else:
        try:
            invite = await discord_client.fetch_invite(snowflake_or_invite_code)
            guild_id = f"{invite.guild.id}"
        except discord.errors.NotFound as exc:
            rprint("Invite code not found:")
            pprint(exc)

        if not is_valid_discord_snowflake(guild_id):
            return web.json_response(
                {
                    "error": "Parameter must be either Guild ID or Invite Code",
                    "code": requests.codes.bad_request,
                },
                status=requests.codes.bad_request,
            )
        guild_info = {"id": guild_id, "name": invite.guild.name}

    if not guild_info:
        guild_info = get_guild_info(guild_id)

    if guild_info is None:
        return web.json_response(
            {"error": "Invalid Guild ID", "code": requests.codes.forbidden},
            status=requests.codes.forbidden,
        )

    rprint(
        f"Resolved [green]{snowflake_or_invite_code}[/green]"
        f" as [yellow]{guild_id}[/yellow] ({guild_info['name']})",
    )

    # TODO: cache resolved invite codes

    # TODO: return past events from MongoDB, not just upcoming events
    guild_events = await fetch_and_store_events_for_guild(guild_id)

    trimmed_guild_info = {
        k: v for k, v in guild_info.items() if k in GUILD_MEANINGFUL_FIELDS
    }
    trimmed_events = [
        {k: v for k, v in gev.items() if k in EVENT_MEANINGFUL_FIELDS}
        for gev in guild_events
    ]

    output = {**trimmed_guild_info, "events": trimmed_events}

    if args.debug:
        static_json_path = Path(FRONTEND_STATIC_PATH) / f"{guild_info['id']}.json"
        with static_json_path.open("w", encoding="utf-8") as json_file:
            json.dump(output, json_file, indent=2)
            rprint(f"Saved static [green]{static_json_path}[/green] file")

    return web.json_response(output)


async def endpoint_handler_ics_feed_generator(request):
    log_http_request(request)

    guild_id = request.match_info["guild_id"]

    if not is_valid_discord_snowflake(guild_id):
        rprint(
            f"[red]Error:[/red] Guild ID [yellow]{guild_id}[/yellow]"
            " failed basic input validation",
        )
        return web.json_response(
            {"error": "Invalid Guild ID", "code": requests.codes.bad_request},
            status=requests.codes.bad_request,
        )

    guild_info = get_guild_info(guild_id)
    if not guild_info:
        rprint(
            f"[red]Error:[/red] Guild [yellow]{guild_id}[/yellow] not found"
            f" (bot not invited?)",
        )
        return web.json_response(
            {
                "error": "Invalid Guild ID or Invite Only Guild",
                "code": requests.codes.forbidden,
            },
            status=requests.codes.forbidden,
        )
    guild_id = f"{guild_info['id']}"

    await fetch_and_store_events_for_guild(guild_id)
    ics_feed = generate_ics_calendar(guild_info).to_ical()
    return web.Response(body=ics_feed, content_type="text/calendar", charset="utf-8")


async def endpoint_handler_suggested_feeds(request):
    log_http_request(request)

    memcache_key = memcache_key_for_suggested_feeds()
    cached_output = memcache_client.get(memcache_key)
    if cached_output is not None:
        rprint("Responding with cached suggested feeds")
        return web.json_response(cached_output)

    rprint("Generating fresh suggested feeds")

    one_month_ago = datetime.now(timezone.utc) - timedelta(days=30)

    query_filter = {
        # don't suggest servers that have no recent events,
        # that would result in empty feeds
        "icalcord_scheduled_start_time": {"$gte": one_month_ago},
    }

    query_projection = dict.fromkeys(EVENT_MEANINGFUL_FIELDS, 1) | {
        "guild_id": 1,
        "_id": 0,  # exclude _id because type ObjectId is not JSON serializable
    }

    recent_events = list(mongo_collection.find(query_filter, query_projection))

    unique_guild_ids = list({ev["guild_id"] for ev in recent_events})

    def format_guild_info(guild_id):
        guild_info = get_guild_info(guild_id)
        guild_info_keys = ["id", "name"]
        guild_info_trimmed = {k: guild_info[k] for k in guild_info_keys}

        guild_events = filter(lambda ev: ev["guild_id"] == guild_id, recent_events)
        guild_info_trimmed["scheduled_events"] = list(guild_events)

        return guild_info_trimmed

    # This line hits Discord API rate limiter.
    # The easy and obvious solution would be to bump up memcache TTL,
    # but it might still hit the limiter due to lack of staggering.
    # Currently, it's _very_ likely for all guild_info caches to be expired
    # when calling this endpoint.
    # The hard solution would be to implement
    # a leaky bucket rate limiter for all Discord API calls
    # and/or reuse old caches when close to the limit,
    # and refresh the oldest caches when there's enough capacity.
    guild_info = map(format_guild_info, unique_guild_ids)

    output = {"guilds": list(guild_info)}

    # I could implement a more sophisticated value generation here, but:
    # - i want to shift processing to client machines as much as possible
    # - this process doesn't know external root URL, unlike JavaScript on the frontend

    memcache_client.set(
        memcache_key,
        output,
        time=config["memcache"]["suggested_feeds_ttl_seconds"],
    )

    if args.debug:
        static_json_path = Path(FRONTEND_STATIC_PATH) / "suggested.json"
        with static_json_path.open("w", encoding="utf-8") as json_file:
            json.dump(output, json_file, indent=2)
            rprint(f"Saved static [green]{static_json_path}[/green] file")

    return web.json_response(output)


async def start_http_server():
    app = web.Application()

    # Verify access to a specific Guild.
    # {id} can be Snowflake or Invite Code
    app.router.add_get(
        "/preview/{snowflake_or_invite_code}.json",
        endpoint_handler_preview,
    )

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
    rprint(f"Fetching events for guild ID: [yellow]{guild_id}[/yellow]")

    # The bot doesn't need to be present on server to fetch events
    # if the server is "Discoverable" (1000+ members)
    # and events are "Public" (default setting).
    # https://docs.discord.com/developers/resources/guild-scheduled-event#list-scheduled-events-for-guild
    discoverable_events_json = retrieve_memcached_current_events_for_guild(guild_id)
    log_events(discoverable_events_json)

    # TODO: do not update MongoDB entries
    # if `discoverable_events_json` was returned from memcache
    if discoverable_events_json is not None:
        for event_json in discoverable_events_json:
            upsert_event(event_json)

    # TODO: Delete upcoming events that are absent from API response?
    # Is there a more reliable way to detect deleted events
    # than relying on API response consistency?

    return discoverable_events_json


@discord_client.event
async def on_ready():
    rprint(f"Logged into Discord as [yellow]{discord_client.user}[/yellow]")
    await start_http_server()
    rprint(
        f"Started HTTP Server"
        f" on {config['frontend']['ip']}:{config['frontend']['port']}",
    )


# TODO: implement a slash command (e.g. /icalcord) to respond with
# .ics feed URL for the server
# and personalized .ics with events user marked as "interested"
@discord_client.event
async def on_message(message):
    if message.author == discord_client.user:
        return

    if message.content.startswith("$hello"):
        await message.channel.send("Hello!")


async def main():
    await discord_client.start(config["discord"]["token"])


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # If we don't catch it,
        # Sentry.io will complain every time the process is killed with ctrl+c
        rprint("[red]Interrupted, exiting...[/red]")
