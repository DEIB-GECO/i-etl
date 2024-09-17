def jsonify_tuple(one_tuple: dict) -> dict:
    if "_id" in one_tuple:
        one_tuple.pop("_id")

    for key in one_tuple:
        if not isinstance(one_tuple[key], str):
            str_value = str(one_tuple[key])
            one_tuple[key] = str_value
    return one_tuple
