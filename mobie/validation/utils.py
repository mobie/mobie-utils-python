

def _assert_equal(val, exp, msg=""):
    if val != exp:
        raise ValueError(msg)


def _assert_true(expr, msg=""):
    if not expr:
        raise ValueError(msg)


def _assert_in(val, iterable, msg=""):
    if val not in iterable:
        raise ValueError(msg)
