CUSTOM_PROPERTY_LIST = {
    'type': 'array',
    'description': ('Specifies key-value pairs of properties associated with '
                    'an object.'),
    'items': {
        'type': 'object',
        'properties': {
            'Name': {'type': 'string'},
            'Value': {'type': ['null', 'string']},
        }
    }
}

ID_FIELD = {
    'type': 'integer',
    'description': ('Read-only legacy identifier for an object. Not '
                    'supported on all objects. Some objects use the '
                    'ObjectID property as the Marketing Cloud unique '
                    'ID.')
}
