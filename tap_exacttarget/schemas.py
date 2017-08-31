event = {
    'type': 'object',
    'properties': {
        'SendID': {
            'type': 'integer',
            'description': 'Contains identifier for a specific send.',
        },
        'EventDate': {
            'type': 'string',
            'format': 'datetime',
            'description': 'Date when a tracking event occurred.',
        },
        'EventType': {
            'type': 'string',
            'description': 'The type of tracking event',
        },
        'SubscriberKey': {
            'type': 'string',
            'description': 'Identification of a specific subscriber.',
        }
    }
}

send = {
    'type': 'object',
    'properties': {
        'CreatedDate': {
            'type': 'string',
            'format': 'date-time',
            'description': 'Read-only date and time of the object\'s creation.',
        },
        'EmailName': {
            'type': 'string',
            'description': 'Specifies the name of an email message associated with a send.',
        },
        'FromAddress': {
            'type': 'string',
            'description': 'Indicates From address associated with a object. Deprecated for email send definitions and triggered send definitions.',
        },
        'FromName': {
            'type': 'string',
            'description': 'Specifies the default email message From Name. Deprecated for email send definitions and triggered send definitions.',
        },
        'ID': {
            'type': 'integer',
            'description': 'Read-only legacy identifier for an object. Not supported on all objects. Some objects use the ObjectID property as the Marketing Cloud unique ID.',
        },
        'IsAlwaysOn': {
            'type': 'boolean',
            'description': 'Indicates whether the request can be performed while the system is is maintenance mode. Avalue of true indicates the system will process the request.',
        },
        'IsMultipart': {
            'type': 'boolean',
            'description': 'Indicates whether the email is sent with Multipart/MIME enabled.',
        },
        'ModifiedDate': {
            'type': 'string',
            'format': 'date-time',
            'description': 'Indicates the last time object information was modified.',
        },
        'PartnerProperties': {
            'type': 'array',
            'description': 'A collection of metadata supplied by client and stored by system - only accessible via API.',
            'items': {
                'type': 'object',
                'properties': {
                    'Name': {
                        'type': 'string',
                    },
                    'Value': {}
                }
            }

        },
        'SendDate': {
            'type': 'string',
            'format': 'date-time',
            'description': 'Indicates the date on which a send occurred. Set this value to have a CST (Central Standard Time) value.',
        },
        'SentDate': {
            'type': 'string',
            'format': 'date-time',
            'description': 'Indicates date on which a send took place.',
        },
        'Status': {
            'type': 'string',
            'description': 'Defines status of object. Status of an address.',
        },
        'Subject': {
            'type': 'string',
            'description': 'Contains subject area information for a message.',
        }
    }
}
