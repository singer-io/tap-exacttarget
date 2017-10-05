import FuelSDK
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import DataAccessObject
from tap_exacttarget.schemas import ID_FIELD, CUSTOM_PROPERTY_LIST
from tap_exacttarget.state import incorporate, save_state

LOGGER = singer.get_logger()


class EmailDataAccessObject(DataAccessObject):
    SCHEMA = {
        'type': 'object',
        'inclusion': 'available',
        'selected': False,
        'properties': {
            'CategoryID': {
                'type': ['null', 'integer'],
                'description': ('Specifies the identifier of the folder '
                                'containing the email.'),
            },
            'CharacterSet': {
                'type': ['null', 'string'],
                'description': ('Indicates encoding used in an email '
                                'message.'),
            },
            'ClonedFromID': {
                'type': ['null', 'integer'],
                'description': ('ID of email message from which the specified '
                                'email message was created.'),
            },
            'ContentAreaIDs': {
                'type': 'array',
                'description': ('Contains information on content areas '
                                'included in an email message.'),
                'items': {
                    'type': 'integer'
                }
            },
            'ContentCheckStatus': {
                'type': ['null', 'string'],
                'description': ('Indicates whether content validation has '
                                'completed for this email message.'),
            },
            'CreatedDate': {
                'type': ['null', 'string'],
                'description': ('Read-only date and time of the object\'s '
                                'creation.'),
            },
            'CustomerKey': {
                'type': ['null', 'string'],
                'description': ('User-supplied unique identifier for an '
                                'object within an object type (corresponds '
                                'to the external key assigned to an object '
                                'in the user interface).'),
            },
            'EmailType': {
                'type': ['null', 'string'],
                'description': ('Defines preferred email type.'),
            },
            'HasDynamicSubjectLine': {
                'type': 'boolean',
                'description': ('Indicates whether email message contains '
                                'a dynamic subject line.'),
            },
            'HTMLBody': {
                'type': ['null', 'string'],
                'description': ('Contains HTML body of an email message.'),
            },
            'ID': ID_FIELD,
            'IsActive': {
                'type': 'boolean',
                'description': ('Specifies whether the object is active.')
            },
            'IsHTMLPaste': {
                'type': 'boolean',
                'description': ('Indicates whether email message was created '
                                'via pasted HTML.')
            },
            'ModifiedDate': {
                'type': ['null', 'string'],
                'description': ('Indicates the last time object '
                                'information was modified.'),
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
            'PreHeader': {
                'type': ['null', 'string'],
                'description': ('Contains text used in preheader of email '
                                'message on mobile devices.')
            },
            'Status': {
                'type': ['null', 'string'],
                'description': ('Defines status of object. Status of an '
                                'address.'),
            },
            'Subject': {
                'type': ['null', 'string'],
                'description': ('Contains subject area information for a '
                                'message.'),
            },
            'SyncTextWithHTML': {
                'type': 'boolean',
                'description': ('Makes the text version of an email contain '
                                'the same content as the HTML version.'),
            },
            'TextBody': {
                'type': ['null', 'string'],
                'description': ('Contains raw text body of a message.'),
            },
            '__AdditionalEmailAttribute1': {'type': ['null', 'string']},
            '__AdditionalEmailAttribute2': {'type': ['null', 'string']},
            '__AdditionalEmailAttribute3': {'type': ['null', 'string']},
            '__AdditionalEmailAttribute4': {'type': ['null', 'string']},
            '__AdditionalEmailAttribute5': {'type': ['null', 'string']},
        }
    }

    TABLE = 'email'
    KEY_PROPERTIES = ['SendID', 'SendType', 'SubscriberKey', 'SendDate']

    def parse_object(self, row):
        to_return = row.copy()
        content_area_ids = []

        for content_area in to_return.get('ContentAreas', []):
            content_area_ids.append(content_area.get('ID'))

        to_return['EmailID'] = to_return.get('Email', {}).get('ID')
        to_return['ContentAreaIDs'] = content_area_ids

        return super(EmailDataAccessObject, self).parse_object(to_return)

    def sync_data(self):
        table = self.__class__.TABLE
        selector = FuelSDK.ET_Email

        search_filter = None
        retrieve_all_since = self.state.get('bookmarks', {}).get(table)

        if retrieve_all_since is not None:
            search_filter = {
                'Property': 'ModifiedDate',
                'SimpleOperator': 'greaterThan',
                'Value': retrieve_all_since
            }

        stream = request('Email',
                         selector,
                         self.auth_stub,
                         search_filter)

        for email in stream:
            email = self.filter_keys_and_parse(email)

            self.state = incorporate(self.state,
                                     table,
                                     'ModifiedDate',
                                     email.get('ModifiedDate'))

            singer.write_records(table, [email])

        save_state(self.state)
