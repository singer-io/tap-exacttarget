

def simple(field, operator, value):
    return {
        'Property': field,
        'SimpleOperator': operator,
        'Value': value
    }


def combine(left, right, operator):
    return {
        'LogicalOperator': operator,
        'LeftOperand': left,
        'RightOperand': right
    }


def between(field, start, end):
    return simple(
        field,
        'between',
        [start, end])
