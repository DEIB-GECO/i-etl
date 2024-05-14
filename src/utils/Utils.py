from functools import wraps
from utils.setup_logger import log


####### Python utils #######

def assert_type(given_type, expected_type):
    if given_type != expected_type:
        raise TypeError("Expected type is " + str(expected_type) + " while given type is " + str(given_type))


def check_type(given_type, expected_type):
    # TODO Nelly: hint annotation: given_type: Type[Class], expected_type: Type[Class]
    return given_type == expected_type


def check_types(types):
    # TODO Nelly: hint annotation: list[tuple]
    for type_var in types:
        log.debug(type_var)
        given_type = type_var[0]
        expected_type = type_var[1]
        if (given_type != expected_type):
            # there is a mismatch between the given and expected variable types
            raise TypeError("Type mismatch. Given type is: " + given_type + "; Expected type is: " + expected_type)
    return True


def decorate_all_functions(function_decorator):
    def decorator(cls):
        for name, obj in vars(cls).items():
            if callable(obj):
                setattr(cls, name, function_decorator(obj))
        return cls

    return decorator


def check_types_before_func(func):
    @wraps(func)
    def wrapper(*args, **kw):
        log.debug(args)
        log.debug(kw)

        try:
            res = func(*args, **kw)
        except:
            raise Exception("Something went wrong")
        return res

    return wrapper
