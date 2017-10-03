import singer


LOGGER = singer.get_logger()


def request(name, selector, auth_stub, search_filter=None):
    cursor = selector()
    cursor.auth_stub = auth_stub

    if search_filter is not None:
        cursor.search_filter = search_filter

    LOGGER.info(
        "Making RETRIEVE call to '{}' endpoint with filter {}."
        .format(name, search_filter))

    response = cursor.get()

    for item in response.results:
        yield item

    while response.more_results:
        LOGGER.info("Getting more results from '{}' endpoint".format(name))

        response = response.getMoreResults()

        for item in response.results:
            yield item

    LOGGER.info("Done retrieving results from '{}' endpoint".format(name))
