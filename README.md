# iCalCord: Discord Events as iCalendar feeds

A Discord bot to export [Guild Scheduled Events](https://docs.discord.com/developers/resources/guild-scheduled-event#list-scheduled-events-for-guild)
as industry standard [iCalendar feeds](https://en.wikipedia.org/wiki/ICalendar) (.ics),
compatible with Google Calendar, Apple iCloud, Microsoft Outlook, Mozilla Thunderbird
and other sane calendar software.

Production instance: <https://icalcord.retromultiplayer.com/>

Discord API does not return past events,
so this bot stores them in MongoDB database and inserts into ICS feeds.

## Development environment

Visual Studio Code Dev Container configuration is included,
and will install required Python dependencies in `postCreateCommand`.
The choice of Debian Bookworm as a container image is intentional,
as it allows to install and run MongoDB and memcached inside DevContainer.
Open the `devcontainer.json` and follow instructions in comments.

1. Create New Application in [Discord Developer Portal](https://discord.com/developers/applications/)
1. Create a MongoDB database with a collection "events"
1. Run memcached instance
1. Copy `config.yaml.example` to `config.yaml` and fill in values.

## Requirements

- Python 3.9
