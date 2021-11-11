import os
import json
import warnings

import jsonschema
import requests


SCHEMA_URLS = {
    "dataset": "https://raw.githubusercontent.com/mobie/mobie.github.io/master/schema/dataset.schema.json",
    "project": "https://raw.githubusercontent.com/mobie/mobie.github.io/master/schema/project.schema.json",
    "source": "https://raw.githubusercontent.com/mobie/mobie.github.io/master/schema/source.schema.json",
    "view": "https://raw.githubusercontent.com/mobie/mobie.github.io/master/schema/view.schema.json",
    "views": "https://raw.githubusercontent.com/mobie/mobie.github.io/master/schema/views.schema.json"
}


def _download_schema():
    folder = os.path.expanduser('~/.mobie')
    os.makedirs(folder, exist_ok=True)

    def _download(address, out_file):
        if os.path.exists(out_file):
            return True
        try:
            r = requests.get(address)
            with open(out_file, 'w') as f:
                f.write(r.content.decode('utf-8'))
            return True
        except Exception:
            return False

    for name, url in SCHEMA_URLS.items():
        out_file = os.path.join(folder, f"{name}.schema.json")
        if not _download(url, out_file):
            return False
    return True


def validate_with_schema(metadata, schema):
    assert isinstance(schema, (str, dict))
    if isinstance(schema, str):
        assert schema in SCHEMA_URLS
        if not _download_schema():
            warnings.warn(f"Could not download the schema from {SCHEMA_URLS[schema]}. Check your internet connection.")
            return
        schema = os.path.expanduser(f'~/.mobie/{schema}.schema.json')
        with open(schema, 'r') as f:
            schema = json.load(f)

        # I hoped this would work, but seems like this doesn't work
        # schema = SCHEMA_URLS[schema]

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
