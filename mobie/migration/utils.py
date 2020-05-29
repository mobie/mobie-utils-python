def string_to_lower(s):
    return s[0].lower() + s[1:]


def dict_to_lower(my_dict):
    return {string_to_lower(k): to_lower(v) for k, v in my_dict.items()}


def list_to_lower(my_list):
    return [to_lower(v) for v in my_list]


def to_lower(v):
    if isinstance(v, dict):
        v = dict_to_lower(v)
    elif isinstance(v, list):
        v = list_to_lower(v)
    elif isinstance(v, str):
        v = string_to_lower(v)
    return v
