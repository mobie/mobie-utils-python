import os
import json
import warnings

import jsonschema
import requests


# TODO jsonschema should be able to do this!
def _download_schema():
    folder = os.path.expanduser('~/.mobie')
    os.makedirs(folder, exist_ok=True)

    source_schema_address = ''
    source_schema_file = os.path.join(folder, 'source.schema.json')

    view_schema_address = ''
    view_schema_file = os.path.join(folder, 'view.schema.json')

    def _download(address, out_file):
        if os.path.exists(out_file):
            return True
        try:
            r = requests.get(address)
            with open(out_file, 'w') as f:
                f.write(r.content)
            return True
        except Exception:
            return False

    if not _download(source_schema_address, source_schema_file):
        return False
    if not _download(view_schema_address, view_schema_file):
        return False
    return True


def validate_with_schema(metadata, schema):
    assert isinstance(schema, (str, dict))
    if isinstance(schema, str):
        assert schema in ("view", "source")
        if not _download_schema():
            warnings.warn("TODO")
            return
        schema = os.path.expanduser(f'~/.mobie/{schema}.schema.json')
        with open(schema, 'r') as f:
            schema = json.load(f)
    jsonschema.validate(instance=metadata, schema=schema)


def _assert_equal(val, exp, msg=""):
    if val != exp:
        raise ValueError(msg)


def _assert_true(expr, msg=""):
    if not expr:
        raise ValueError(msg)


def _assert_in(val, iterable, msg=""):
    if val not in iterable:
        raise ValueError(msg)
