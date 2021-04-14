import os
from jsonschema import ValidationError
from pybdv.metadata import get_name
from .utils import _assert_true, _assert_equal, validate_with_schema


def validate_source_metadata(name, metadata, dataset_folder=None,
                             assert_true=_assert_true, assert_equal=_assert_equal):
    # static validation with json schema
    try:
        validate_with_schema(metadata, 'source')
    except ValidationError as e:
        msg = f"{e}"
        assert_true(False, msg)

    source_type = list(metadata.keys())[0]
    metadata = metadata[source_type]
    # dynamic validation of paths
    if dataset_folder is not None:
        data_locations = metadata['imageDataLocations']
        for storage, location in data_locations.items():
            path = os.path.join(dataset_folder, location)
            msg = f"Could not find xml for {storage} at {path}"
            assert_true(os.path.exists(path), msg)
            bdv_name = get_name(path, setup_id=0)
            assert_equal(name, bdv_name)
        if 'tableDataLocation' in metadata:
            table_folder = os.path.join(dataset_folder, metadata['tableDataLocation'])
            msg = f"Could not find table root folder at {table_folder}"
            assert_true(os.path.isdir(table_folder), msg)
            default_table = os.path.join(table_folder, 'default.tsv')
            msg = f"Could not find default table at {default_table}"
            assert_true(os.path.exists(default_table), msg)

    # dynamic validation of view metadata
    view = metadata['view']
    validate_view_metadata(view, sources=[name], assert_true=assert_true)


def validate_view_metadata(view, sources=None, assert_true=_assert_true):
    # static validation with json schema
    try:
        validate_with_schema(view, 'view')
    except ValidationError as e:
        msg = f"{e}"
        assert_true(False, msg)

    # dynamic validation of sources
    all_display_sources = []
    displays = view.get("sourceDisplays")
    if displays is not None:
        for display in displays:
            display_metadata = list(display.values())[0]
            display_sources = display_metadata["sources"]
            all_display_sources.extend(display_sources)
            if sources is not None:
                wrong_sources = list(set(display_sources) - set(sources))
                msg = f"Found wrong sources {wrong_sources} in sourceDisplay"
                assert_true(len(wrong_sources) == 0, msg)

    # TODO validate table root location for auto grid
    source_transformations = view.get("sourceTransformations")
    if source_transformations is not None:
        all_display_sources = set(all_display_sources)
        for transform in source_transformations:
            transform_metadata = list(transform.values())[0]
            transform_name = transform_metadata['name']
            transform_sources = transform_metadata["sources"]
            wrong_sources = list(set(transform_sources) - all_display_sources)
            msg = f"Found wrong sources {wrong_sources} in transform {transform_name}"
            assert_true(len(wrong_sources) == 0, msg)
