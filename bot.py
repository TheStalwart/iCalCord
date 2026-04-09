"""iCalCord: Discord Events as iCalendar feeds.

A Discord bot to export Guild Scheduled Events
as industry standard iCalendar feeds (.ics),
compatible with Google Calendar, Apple iCloud, Microsoft Outlook, Mozilla Thunderbird
and other sane calendar software.
"""

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
from icalendar import Calendar, Event, FreeBusy, vRecur
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
    "recurrence_rule",
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


def initialize_sentry_sdk() -> None:
    """Initialize Sentry SDK using configuration values.

    Reads the Sentry configuration from the global config dictionary
    and initializes sentry_sdk with the configured DSN and sampling rates.
    When running in debug mode, initialization is skipped
    unless a DSN is explicitly configured.

    When debugging, silently ignores failure.
    """
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


def memcache_key_for_guild_events(guild_id) -> str:
    """Generate a memcache key for guild events.

    Parameters
    ----------
    guild_id : str
        The ID of the guild.

    Returns
    -------
    str
        The memcache key for the guild's events.

    """
    return f"{config['memcache']['key_prefix']}_guild_events_{guild_id}"


def memcache_key_for_guild_info(guild_id) -> str:
    """Generate a memcache key for guild info.

    Parameters
    ----------
    guild_id : str
        The ID of the guild.

    Returns
    -------
    str
        The memcache key for the guild's info.

    """
    return f"{config['memcache']['key_prefix']}_guild_info_{guild_id}"


def memcache_key_for_suggested_feeds() -> str:
    """Return a memcache key for generated "suggested feeds" output."""
    return f"{config['memcache']['key_prefix']}_suggested_feeds"


def log_guild_info(guild_info) -> None:
    """Log the guild ID and name for debugging and informational output.

    Parameters
    ----------
    guild_info : dict
        Dictionary containing guild data,
        expected to include the 'id' and 'name' keys.

    """
    guild_id = guild_info.get("id", "Unknown")
    guild_name = guild_info.get("name", "Unknown")

    rprint(
        f"Guild name for [yellow]{guild_id}[/yellow]: [green]{guild_name}[/green]",
    )


def discord_api_http_request(url):
    """Perform a direct Discord API GET request with the configured bot token.

    discord.py internal intent-based event-listening architecture
    collides with the fact
    Discoverable guilds allow fetching EXTERNAL (entity_type: 3) scheduled event data
    without the bot being present on the server.
    https://docs.discord.com/developers/resources/guild-scheduled-event
    https://deepwiki.com/Rapptz/discord.py/7.3-scheduled-events

    I'm not sure this is intentional:
    https://discord.com/channels/613425648685547541/1130595287078015027/1491755830247293038

    I talked to discord.py devs,
    and they suggested to create a thread in #bikeshedding category
    of their official Discord server.
    https://discord.com/channels/336642139381301249/1491295357601321030

    Calling internal aiohttp client directly with these kinds of calls:
    ```
    discord_client.http.get_scheduled_events(guild_id, with_user_count=True)
    ```
    triggers rate-limiter bug and stalls for 10 seconds before returning the value.
    So two more discussion thread were created:
    https://discord.com/channels/336642139381301249/1491283868211351735
    https://discord.com/channels/336642139381301249/1491288197324476587

    There's another thing i don't like about the above function,
    is that it returns an unserializable struct instead of raw JSON response,
    which makes it inconvenient to store in MongoDB.
    It also drops recurrence_rule field,
    which is a critical piece of information for generating correct ICS output.
    There's a PR to add support for recurrence_rule field in the internal client,
    and it's been sitting there unmerged for years:
    https://github.com/Rapptz/discord.py/pull/9685

    It's possible to bypass the response parsing with this function:
    ```
    data = await discord_client.http.request(
        discord.http.Route("GET", f"/guilds/{guild_id}/scheduled-events"),
    )
    ```
    but it trips on the same rate-limiter bug as `get_scheduled_events()` function.

    So until rate-limiter issues are resolved, i'm bypassing discord.py entirely,
    but there are issues with this approach as well,
    such as lack of rate limit handling,
    and the blocking nature of requests library,
    which can stall aiohttp server and discord.py if Discord API is slow to respond.

    If rate limiting and/or blocking becomes a bigger issue before discord.py is fixed,
    i could drop-in replace requests with aiohttp library.
    https://docs.aiohttp.org/en/stable/http_request_lifecycle.html
    Or try to fork/hack/fix discord.py's internal aiohttp client.

    As of April 2026, i resolved a rate-limiter hit by making fewer API calls
    and not hoarding data that might never be used.

    Parameters
    ----------
    url : str
        The Discord API endpoint URL to query.

    Returns
    -------
    requests.Response
        The HTTP response returned by the Discord API.

    """
    headers = {"Authorization": f"Bot {config['discord']['token']}"}
    return requests.get(
        url,
        headers=headers,
        timeout=config["discord"]["http_request_timeout_seconds"],
    )


def is_valid_discord_snowflake(snowflake: str | None) -> bool:
    """Validate whether a value is a plausible Discord Snowflake.

    A Discord Snowflake is an unsigned 64-bit integer
    represented as a decimal string.
    This function performs lightweight validation
    suitable for input sanitization and basic format checks.

    The validation criteria are:
    - The value is not None
    - The value contains digits only
    - The length is within the expected Snowflake range (16-20 digits)

    Notes
    -----
    This function does not fully validate Snowflake semantics
    (e.g., timestamp correctness).
    It only checks structural plausibility.

    References
    ----------
    - https://docs.discord.com/developers/reference#snowflakes
    - https://medium.com/netcord/discord-snowflake-explained-id-generation-process-a468be00a570
    - https://discordutils.com/snowflake-decoder

    Parameters
    ----------
    snowflake : str | None
        The value to validate.

    Returns
    -------
    bool
        True if the value appears to be a valid Snowflake identifier,
        otherwise False.

    """
    if snowflake is None:
        return False

    min_length = 16  # realistically, 17 should be the shortest ever
    max_length = 20  # expected maximum length of a 64bit integer
    return snowflake.isdigit() and (min_length <= len(snowflake) <= max_length)


def get_guild_info(guild_id):
    """Retrieve guild information for a given guild ID.

    Attempts to fetch guild info from Discord client's cache first.
    If not available, checks memcache,
    and if not cached, fetches from Discord API and caches the result.

    Parameters
    ----------
    guild_id : str
        The ID of the guild to retrieve information for.

    Returns
    -------
    dict or None
        A dictionary containing guild 'id' and 'name' if successful,
        or None if the fetch failed.

    """
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
        f"[red]Error:[/red] Failed ({response.status_code}) to fetch Discord guild info"
        f" for guild ID: [yellow]{guild_id}[/yellow]: {response.content}",
    )

    sentry_sdk.capture_message(
        f"Request to {url} failed"
        f" with status code {response.status_code}:"
        f" {response.content}",
    )

    return None


def log_sentry_init_failure(exc: Exception) -> None:
    """Log a warning when Sentry initialization fails.

    Parameters
    ----------
    exc : Exception
        The exception raised by the Sentry SDK initialization attempt.

    """
    rprint("[red]Warning:[/red] Sentry initialization failed!")
    pprint(exc)


def log_events(events: list[dict]) -> None:
    """Log a list of Discord scheduled events for debugging output.

    Parameters
    ----------
    events : list
        List of event dictionaries containing at least
        'scheduled_start_time', 'id', and 'name' keys.

    """
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
    """Retrieve the list of users subscribed to a specific scheduled event.

    This endpoint works for both Discoverable and Invite Only servers.

    Parameters
    ----------
    guild_id : str
        The ID of the guild.
    event_id : str
        The ID of the scheduled event.

    Returns
    -------
    list or None
        A list of user dictionaries if successful, or None if the fetch failed.

    """
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
        f"[red]Error:[/red] Failed ({response.status_code}) to fetch subscribed users"
        f" for event ID: [yellow]{event_id}[/yellow]: {response.content}",
    )

    sentry_sdk.capture_message(
        f"Request to {url} failed"
        f" with status code {response.status_code}:"
        f" {response.content}",
    )

    return None


def retrieve_memcached_upcoming_events_for_guild(
    guild_id,
    *,
    include_subscribed_users=False,
):
    """Retrieve upcoming scheduled events for a guild, using cache if available.

    Attempts to fetch events from memcache first.
    If not cached, fetches from Discord API,
    optionally enriches events with subscribed users,
    caches the result, and returns it.

    Parameters
    ----------
    guild_id : str
        The ID of the guild to retrieve events for.

    include_subscribed_users : bool, optional
        Whether to include subscribed user data for each event.
        Defaults to False.

    Returns
    -------
    list or None
        A list of event dictionaries if successful,
        or None if the fetch failed.

    """
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
        if include_subscribed_users:
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
        f"[red]Error:[/red] Failed ({response.status_code}) to fetch Discord events"
        f" for guild ID [yellow]{guild_id}[/yellow]: {response.content}",
    )

    sentry_sdk.capture_message(
        f"Request to {url} failed"
        f" with status code {response.status_code}:"
        f" {response.content}",
    )

    return None


def diff_event(existing: dict | None, fresh: dict) -> bool:
    """Whether or not the event was edited.

    Compares the existing event data with the new event data
    and returns True if any meaningful fields have changed.

    This is primarily to address the `scheduled_start_time` field
    always pointing to the next instance of recurring event,
    even though the event itself was not edited.

    Parameters
    ----------
    existing : dict | None
        The existing event data, typically retrieved from MongoDB.
    fresh : dict
        The fresh event data, typically retrieved from the Discord API.

    Returns
    -------
    bool
        Whether or not the event was edited
        in a way that affects the generated ICS output.

    """
    meaningful_fields = EVENT_MEANINGFUL_FIELDS.copy()
    if fresh.get("recurrence_rule"):
        # For recurring events,
        # `scheduled_start_time` always points to the next occurrence,
        # which is useful when previewing the event data,
        # but does not indicate the event was edited,
        # and also isn't used in ICS output
        # because `recurrence_rule.start` is used instead.
        meaningful_fields.remove("scheduled_start_time")

    return existing is None or any(
        fresh.get(field) != existing.get(field) for field in meaningful_fields
    )


def upsert_event(event_data: dict) -> None:
    """Upsert a Discord scheduled event into the MongoDB collection.

    Checks for changes in meaningful fields,
    updates or inserts the event,
    and logs the action.
    Converts ISO 8601 timestamps to datetime objects.

    Parameters
    ----------
    event_data : dict
        The event data dictionary from the Discord API response,
        containing fields like 'id', 'name', 'scheduled_start_time', etc.

    Raises
    ------
    PyMongoError
        If a database operation fails.

    """
    try:
        existing = mongo_collection.find_one({"id": event_data["id"]})

        # Check if any of the meaningful fields changed
        data_changed = diff_event(existing, event_data)

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


def discord_recurrence_rule_to_vrecur(recurrence_rule: dict) -> vRecur:
    """Convert a Discord API recurrence rule to an iCalendar vRecur object.

    Discord API docs claim their recurrence rules are based on iCalendar RFC 5545:
    https://docs.discord.com/developers/resources/guild-scheduled-event#guild-scheduled-event-recurrence-rule-object

    But the mapping is not 1:1, some ENUMs need to be converted
    https://icalendar.org/iCalendar-RFC-5545/3-8-5-3-recurrence-rule.html

    Parameters
    ----------
    recurrence_rule : dict
        The recurrence rule data from the Discord API response,
        expected to include 'frequency' and 'interval' keys.

    Returns
    -------
    vRecur
        The iCalendar vRecur object representing the recurrence rule.

    """
    params = {}

    frequency = recurrence_rule.get("frequency")
    if frequency:
        params["FREQ"] = ["YEARLY", "MONTHLY", "WEEKLY", "DAILY"][frequency]

    interval = recurrence_rule.get("interval")
    if interval:
        params["INTERVAL"] = interval

    by_weekday = recurrence_rule.get("by_weekday")
    if by_weekday:
        # Discord API represents weekdays as integers 0-6 (Monday-Sunday),
        # while iCalendar uses MO, TU, WE, TH, FR, SA, SU.
        weekday_mapping = {
            0: "MO",
            1: "TU",
            2: "WE",
            3: "TH",
            4: "FR",
            5: "SA",
            6: "SU",
        }
        params["BYDAY"] = [weekday_mapping[day] for day in by_weekday]

    return vRecur(params)


def generate_ics_vevent(event: dict) -> Event:
    """Convert a Discord scheduled event document into an iCalendar VEVENT.

    Parameters
    ----------
    event : dict
        The event data from MongoDB or Discord API response.

    Returns
    -------
    icalendar.Event
        The VEVENT component representing the event.

    """
    ics_event = Event()
    ics_event.uid = f"{event['id']}@icalcord"
    ics_event.summary = event["name"]
    ics_event.stamp = event.get("icalcord_updated_time")

    rrules = event.get("recurrence_rule")
    if rrules:
        ics_event.start = datetime.fromisoformat(rrules["start"])
        ics_event.add("RRULE", discord_recurrence_rule_to_vrecur(rrules))
    elif event.get("icalcord_scheduled_start_time"):
        # For non-recurring events, use the scheduled_start_time as the DTSTART.
        # Value resolver is a bit of a mess
        # due to me overwriting scheduled_start_time with a derivative datetime value
        # for early records in the database,
        # but i'm not gonna throw away old event data (right now)
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
    """Generate an ICS feed for the given guild.

    Parameters
    ----------
    guild_info : dict
        Dictionary containing guild information,
        expected to include the 'id' and 'name' keys.

    Returns
    -------
    icalendar.Calendar
        The Calendar object representing the ICS feed.

    """
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
    """Log an incoming HTTP request with the client IP and requested URL.

    Parameters
    ----------
    request : aiohttp.web.Request
        The incoming HTTP request object.

    """
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
    """Serve the frontend index page and log the incoming HTTP request."""
    log_http_request(request)

    return web.FileResponse(FRONTEND_ROOT_PATH / "index.html")


async def serve_legacy_favicon(request):
    """Serve the legacy favicon.ico file for compatibility with older clients."""
    log_http_request(request)

    return web.FileResponse(FRONTEND_STATIC_PATH / "logo" / "favicon.ico")


async def endpoint_handler_preview(request) -> web.Response:
    """Preview event data by resolving a guild snowflake or invite code."""
    log_http_request(request)

    snowflake_or_invite_code = request.match_info["snowflake_or_invite_code"]
    guild_id = None
    guild_info = None

    # Define generic error message in case we fail to resolve a more specific one later
    error = "Parameter must be either Guild ID or Invite Code"

    if is_valid_discord_snowflake(snowflake_or_invite_code):
        guild_id = snowflake_or_invite_code
    else:
        try:
            invite = await discord_client.fetch_invite(snowflake_or_invite_code)
            guild_id = f"{invite.guild.id}"
        except discord.errors.NotFound as exc:
            rprint("Invite code not found:")
            pprint(exc)
            error = exc.text

        if not is_valid_discord_snowflake(guild_id):
            return web.json_response(
                {
                    "error": error,
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
    guild_events = await fetch_and_store_upcoming_events_for_guild(guild_id)

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


async def endpoint_handler_ics_feed_generator(request) -> web.Response:
    """Handle requests to generate an ICS feed for a guild.

    Parameters
    ----------
    request : aiohttp.web.Request
        The incoming HTTP request, expected to contain a guild_id path parameter.

    Returns
    -------
    aiohttp.web.Response
        A response containing the generated iCalendar feed,
        or a JSON error response if the guild ID is invalid or not found.

    """
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

    await fetch_and_store_upcoming_events_for_guild(guild_id)
    ics_feed = generate_ics_calendar(guild_info).to_ical()
    return web.Response(body=ics_feed, content_type="text/calendar", charset="utf-8")


async def endpoint_handler_suggested_feeds(request) -> web.Response:
    """Handle requests for suggested feeds and return cached or generated JSON.

    Parameters
    ----------
    request : aiohttp.web.Request
        The incoming HTTP request object.

    Returns
    -------
    aiohttp.web.Response
        A JSON response containing suggested guild feed information.

    """
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


async def start_http_server() -> None:
    """Start the aiohttp web server with configured routes and static file serving.

    Sets up the HTTP server with routes for feed generation, guild preview,
    and suggested feeds endpoints. Also serves the frontend static files.

    """
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

    # /favicon.ico for legacy browsers
    app.router.add_get("/favicon.ico", serve_legacy_favicon)

    # Serve entire /frontend/static directory
    app.router.add_static("/static/", path=FRONTEND_STATIC_PATH)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, config["frontend"]["ip"], config["frontend"]["port"])
    await site.start()


async def fetch_and_store_upcoming_events_for_guild(guild_id):
    """Fetch upcoming guild events from Discord API, and return the event list.

    The bot doesn't need to be present on server to fetch events
    if the server is "Discoverable" (1000+ members)
    and events are "Public" (default setting).

    https://docs.discord.com/developers/resources/guild-scheduled-event#list-scheduled-events-for-guild

    Parameters
    ----------
    guild_id : str
        The ID of the guild to fetch events for.

    Returns
    -------
    list or None
        The list of upcoming scheduled events for the guild,
        or None if the fetch failed.

    """
    rprint(f"Fetching upcoming events for guild ID: [yellow]{guild_id}[/yellow]")

    discoverable_events_json = retrieve_memcached_upcoming_events_for_guild(guild_id)
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
async def on_ready() -> None:
    """Handle the Discord client ready event and start the HTTP server.

    This event is called once the Discord client has successfully connected.
    It logs the bot Discord credentials
    and starts the aiohttp server with customer-facing endpoints.
    """
    rprint(f"Logged into Discord as [yellow]{discord_client.user}[/yellow]")
    await start_http_server()

    clickable_url_suffix = (
        f"aka http://localhost:{config['frontend']['port']}"
        if config["frontend"]["ip"] == "0.0.0.0"  # noqa: S104
        else ""
    )
    rprint(
        "Started HTTP Server"
        f" on http://{config['frontend']['ip']}:{config['frontend']['port']}",
        clickable_url_suffix,
    )


@discord_client.event
async def on_message(message: discord.Message) -> None:
    """Handle incoming Discord messages and respond to bot commands.

    TODO: implement a slash command (e.g. /icalcord)
    to respond with .ics feed URL for the server
    and personalized .ics with events user marked as "interested"

    WARNING: Discord.py docs state this is a bad idea,
    and individual slash-command handlers should be implemented instead
    https://discordpy.readthedocs.io/en/latest/faq.html#why-does-on-message-make-my-commands-stop-working

    Parameters
    ----------
    message : discord.Message
        The message object received from Discord.

    """
    if message.author == discord_client.user:
        return

    if message.content.startswith("$hello"):
        await message.channel.send("Hello!")


async def main():
    """Start the Discord client using the configured bot token."""
    await discord_client.start(config["discord"]["token"])


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # If we don't catch it,
        # Sentry.io will complain every time the process is killed with ctrl+c
        rprint("[red]Interrupted, exiting...[/red]")
