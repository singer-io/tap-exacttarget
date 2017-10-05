import FuelSDK
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import DataAccessObject
from tap_exacttarget.schemas import ID_FIELD, CUSTOM_PROPERTY_LIST
from tap_exacttarget.state import incorporate, save_state


LOGGER = singer.get_logger()


class ContentAreaDataAccessObject(DataAccessObject):

    SCHEMA = {
        'type': 'object',
        'inclusion': 'available',
        'selected': False,
        'properties': {
            'BackgroundColor': {
                'type': ['null', 'string'],
                'description': 'Indicates background color of content area',
            },
            'BorderColor': {
                'type': ['null', 'string'],
                'description': ('Indicates color of border surrounding '
                                'content area'),
            },
            'BorderWidth': {
                'type': ['null', 'integer'],
                'description': ('Indicates pixel width of border '
                                'surrounding content area'),
            },
            'CategoryID': {
                'type': ['null', 'integer'],
                'description': 'Specifies the identifier of the folder.',
            },
            'Cellpadding': {
                'type': ['null', 'integer'],
                'description': ('Indicates pixel value of padding '
                                'around content area'),
            },
            'Cellspacing': {
                'type': ['null', 'integer'],
                'description': ('Indicates pixel value of spacing '
                                'for content area'),
            },
            'Content': {
                'type': ['null', 'string'],
                'description': ('Identifies content contained in '
                                'a content area.'),
            },
            'CorrelationID': {
                'type': ['null', 'string'],
                'description': ('Identifies correlation of objects across '
                                'several requests.'),
            },
            'CreatedDate': {
                'type': ['null', 'string'],
                'description': ('Read-only date and time of the object\'s'
                                'creation.'),
            },
            'CustomerKey': {
                'type': ['null', 'string'],
                'description': ('User-supplied unique identifier for an '
                                'object within an object type (corresponds '
                                'to the external key assigned to an object '
                                'in the user interface).'),
            },
            'FontFamily': {
                'type': ['null', 'string'],
                'description': 'Indicates font family used in content area',
            },
            'HasFontSize': {
                'type': 'boolean',
                'description': ('Indicates whether the content area includes '
                                'a specified font size or not'),
            },
            'ID': ID_FIELD,
            'IsBlank': {
                'type': 'boolean',
                'description': ('Indicates if specified content area '
                                'contains no content.'),
            },
            'IsDynamicContent': {
                'type': 'boolean',
                'description': ('Indicates if specific content area '
                                'contains dynamic content.'),
            },
            'IsLocked': {
                'type': 'boolean',
                'description': ('Indicates if specific email content area '
                                'within an Enterprise or Enterprise 2.0 '
                                'account is locked and cannot be changed by '
                                'subaccounts.'),
            },
            'IsSurvey': {
                'type': 'boolean',
                'description': ('Indicates whether a specific content area '
                                'contains survey questions.'),
            },
            'Key': {
                'type': ['null', 'string'],
                'description': ('Specifies key associated with content area '
                                'in HTML body. Relates to the Email object '
                                'via a custom type.'),
            },
            'ModifiedDate': {
                'type': ['null', 'string'],
                'description': ('Indicates the last time object information '
                                'was modified.'),
            },
            'Name': {
                'type': ['null', 'string'],
                'description': 'Name of the object or property.',
            },
            'ObjectID': {
                'type': ['null', 'string'],
                'description': ('System-controlled, read-only text string '
                                'identifier for object.'),
            },
            'PartnerProperties': CUSTOM_PROPERTY_LIST,
            'Width': {
                'type': ['null', 'integer'],
                'description': 'Indicates pixel width of content area',
            },
        }
    }

    TABLE = 'content_area'
    KEY_PROPERTIES = ['ObjectID']

    def sync_data(self):
        table = self.__class__.TABLE
        selector = FuelSDK.ET_ContentArea

        search_filter = None
        retrieve_all_since = self.state.get('bookmarks', {}).get(table)

        if retrieve_all_since is not None:
            search_filter = {
                'Property': 'ModifiedDate',
                'SimpleOperator': 'greaterThan',
                'Value': retrieve_all_since
            }

        stream = request('ContentAreaDataAccessObject',
                         selector,
                         self.auth_stub,
                         search_filter)

        for content_area in stream:
            content_area = self.filter_keys_and_parse(content_area)

            self.state = incorporate(self.state,
                                     table,
                                     'ModifiedDate',
                                     content_area.get('ModifiedDate'))

            singer.write_records(table, [content_area])

        save_state(self.state)
