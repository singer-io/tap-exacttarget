import datetime
import singer

from tap_exacttarget.filters import between

LOGGER = singer.get_logger()
DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# checks if the 'date_value' is less than or equal to now date
def before_now(date_value):
    return (datetime.datetime.strptime(date_value, DATE_FORMAT) <=
            datetime.datetime.utcnow())

# increment date by the values present in 'unit'
def increment_date(date_value, unit=None):
    if unit is None:
        unit = {'days': 1}

    date_obj = datetime.datetime.strptime(date_value, DATE_FORMAT)

    incremented_date_obj = date_obj + datetime.timedelta(**unit)

    return datetime.datetime.strftime(incremented_date_obj, DATE_FORMAT)

# returns the filter with start date, end date and the field
# {
#   "field",
#   "between",
#   ["start", "end"]
# }
def get_date_page(field, start, unit):
    return between(field, start, increment_date(start, unit))
