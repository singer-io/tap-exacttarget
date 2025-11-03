from singer import get_logger, utils

from tap_exacttarget.client import Client
from tap_exacttarget.discover import discover
from tap_exacttarget.sync import sync

REQUIRED_CONFIG_KEYS = ["client_id", "client_secret", "start_date", "tenant_subdomain"]
LOGGER = get_logger()


@utils.handle_top_exception(LOGGER)
def main():
    """Performs sync and discovery."""
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    client = Client(args.config)

    if args.discover:
        discover(args.config, client).dump()
    else:
        sync(client, args.catalog or discover(args.config, client), args.state)


if __name__ == "__main__":
    main()
