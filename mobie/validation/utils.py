"""Helper functions for validation.
"""
import os
import json
import warnings
from typing import Callable, Dict, Optional

import jsonschema
import requests
import s3fs


SCHEMA_URLS = {
    "dataset": "https://raw.githubusercontent.com/mobie/mobie.github.io/master/schema/dataset.schema.json",
    "project": "https://raw.githubusercontent.com/mobie/mobie.github.io/master/schema/project.schema.json",
    "source": "https://raw.githubusercontent.com/mobie/mobie.github.io/master/schema/source.schema.json",
    "view": "https://raw.githubusercontent.com/mobie/mobie.github.io/master/schema/view.schema.json",
    "views": "https://raw.githubusercontent.com/mobie/mobie.github.io/master/schema/views.schema.json",
    # NGFF / OME-Zarr image schemas. 'NGFF' is v0.4 (zarr v2); 'NGFF_0.5' is v0.5 (zarr v3),
    # which references the '_version' schema, so both files are downloaded and resolved together.
    "NGFF": "https://raw.githubusercontent.com/ome/ngff/7ac3430c74a66e5bcf53e41c429143172d68c0a4/schemas/image.schema",
    "NGFF_0.5": "https://ngff.openmicroscopy.org/0.5/schemas/image.schema",
    "NGFF_0.5_version": "https://ngff.openmicroscopy.org/0.5/schemas/_version.schema",
}
"""@private
"""


def _download_schema():
    folder = os.path.expanduser("~/.mobie")
    os.makedirs(folder, exist_ok=True)

    def _download(address, out_file):
        if os.path.exists(out_file):
            return True
        try:
            r = requests.get(address, timeout=30)
            with open(out_file, "w") as f:
                f.write(r.content.decode("utf-8"))
            return True
        except Exception:
            return False

    for name, url in SCHEMA_URLS.items():
        out_file = os.path.join(folder, f"{name}.schema.json")
        if not _download(url, out_file):
            return False
    return True


def validate_with_schema(metadata: Dict, schema: str) -> None:
    """Validate that a dictionary with MoBIE metadata adheres to the given json schema.

    Raises a JsonSchemaValidation error if the metadata is not spec complient.

    Args:
        metadata: The dictionary with MoBIE metadata.
        schema: The name of the schema. One of 'dataset', 'project', 'source', 'view', 'views'.

    """
    assert isinstance(schema, (str, dict))
    if isinstance(schema, str):
        assert schema in SCHEMA_URLS
        if not _download_schema():
            warnings.warn(f"Could not download the schema from {SCHEMA_URLS[schema]}. Check your internet connection.")
            return
        schema = os.path.expanduser(f"~/.mobie/{schema}.schema.json")
        with open(schema, "r") as f:
            schema = json.load(f)
    jsonschema.validate(instance=metadata, schema=schema)


def load_json_from_s3(address):
    """@private
    """
    server = "/".join(address.split("/")[:3])
    root_path = "/".join(address.split("/")[3:-1])
    fname = address.split("/")[-1]
    fs = s3fs.S3FileSystem(anon=True, client_kwargs={"endpoint_url": server})
    store = s3fs.S3Map(root=root_path, s3=fs)
    attrs = store[fname]
    attrs = json.loads(attrs.decode("utf-8"))
    return attrs


#
# helpers for reading NGFF / OME-Zarr metadata across zarr v2 (v0.4) and v3 (v0.5) layouts
#


def ngff_multiscales(attrs: Dict) -> list:
    """Return the 'multiscales' list from ome.zarr group attributes.

    Handles both the NGFF v0.4 layout (multiscales at the top level, from a `.zattrs` file) and
    the v0.5 layout (multiscales nested under an 'ome' key, from a `zarr.json` `attributes` block).

    Args:
        attrs: The ome.zarr group attributes.

    Returns:
        The multiscales list.
    """
    if "ome" in attrs:
        return attrs["ome"]["multiscales"]
    return attrs["multiscales"]


def ngff_version(attrs: Dict) -> Optional[str]:
    """Return the NGFF version from ome.zarr group attributes (v0.4 or v0.5 layout).

    Args:
        attrs: The ome.zarr group attributes.

    Returns:
        The NGFF version string, or None if it is not set.
    """
    if "ome" in attrs:
        return attrs["ome"].get("version")
    multiscales = attrs.get("multiscales", [])
    return multiscales[0].get("version") if multiscales else None


def load_ngff_group_attrs(read_json: Callable[[str], Optional[Dict]]) -> Optional[Dict]:
    """Load ome.zarr group attributes, handling zarr v2 (`.zattrs`) and v3 (`zarr.json`).

    This works for both local and remote (s3) data by supplying an appropriate reader.

    Args:
        read_json: A callable that reads and parses a sub-file (by name) of the ome.zarr group,
            returning the parsed dictionary or None if it does not exist.

    Returns:
        The group attributes: `{"multiscales": [...]}` for v0.4 or `{"ome": {...}}` for v0.5.
        None if neither the `.zattrs` nor the `zarr.json` metadata can be read.
    """
    zattrs = read_json(".zattrs")
    if zattrs is not None:
        return zattrs
    zarr_json = read_json("zarr.json")
    if zarr_json is not None:
        return zarr_json.get("attributes", {})
    return None


def load_ngff_array_shape(read_json: Callable[[str], Optional[Dict]]) -> Optional[list]:
    """Load an ome.zarr array's shape, handling zarr v2 (`.zarray`) and v3 (`zarr.json`).

    Args:
        read_json: A callable that reads and parses a sub-file (by name) of the array node,
            returning the parsed dictionary or None if it does not exist.

    Returns:
        The array shape, or None if neither the `.zarray` nor the `zarr.json` metadata can be read.
    """
    zarray = read_json(".zarray")
    if zarray is not None:
        return zarray["shape"]
    zarr_json = read_json("zarr.json")
    if zarr_json is not None:
        return zarr_json["shape"]
    return None


def _validate_ngff_v05(attrs: Dict) -> None:
    """Validate v0.5 ome.zarr attributes, resolving the external '_version' schema reference."""
    from referencing import Registry, Resource
    from jsonschema.validators import validator_for

    if not _download_schema():
        warnings.warn("Could not download the NGFF v0.5 schema. Check your internet connection.")
        return
    folder = os.path.expanduser("~/.mobie")
    with open(os.path.join(folder, "NGFF_0.5.schema.json")) as f:
        image_schema = json.load(f)
    with open(os.path.join(folder, "NGFF_0.5_version.schema.json")) as f:
        version_schema = json.load(f)

    resources = [Resource.from_contents(image_schema), Resource.from_contents(version_schema)]
    registry = Registry().with_resources([(res.id(), res) for res in resources])
    validator_cls = validator_for(image_schema)
    validator_cls(image_schema, registry=registry).validate(attrs)


def validate_ngff_metadata(attrs: Dict, version: Optional[str] = None) -> None:
    """Validate ome.zarr group attributes against the matching NGFF schema (v0.4 or v0.5).

    Args:
        attrs: The ome.zarr group attributes, as returned by `load_ngff_group_attrs`
            (`{"multiscales": [...]}` for v0.4, `{"ome": {...}}` for v0.5).
        version: The NGFF version. If None, it is inferred from `attrs`.
    """
    if version is None:
        version = ngff_version(attrs)
    if version == "0.5":
        _validate_ngff_v05(attrs)
    else:
        validate_with_schema(attrs, "NGFF")


def _assert_equal(val, exp, msg=""):
    if val != exp:
        raise ValueError(msg)


def _assert_true(expr, msg=""):
    if not expr:
        raise ValueError(msg)


def _assert_in(val, iterable, msg=""):
    if val not in iterable:
        raise ValueError(msg)
