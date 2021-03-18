import os
from .utils import _assert_true


def validate_source_metadata(name, metadata, dataset_folder=None,
                             assert_true=_assert_true):
    # TODO static validate with json schema

    # dynamic validation of entries

    # TODO can this be done in json schema? (checking string format)
    menu_item = metadata["menuItem"]
    if not isinstance(menu_item, str) and len(menu_item.split("/") != 2):
        assert_true(False, f"The field menu_item must have the format '<MENU>/<ENTRY>', got {menu_item}")

    # TODO can this be done in json schema? (dependent fields)
    if "tableRootLocation" in metadata and metadata["type"] != "segmentation":
        msg = "Invalid parameter combination: a table folder may only be specified for sources of type segmentation"
        assert_true(False, msg)

    # dynamic validation of paths
    if dataset_folder is not None:
        image_location = metadata['imageLocation']
        for storage, location in image_location.items():
            path = os.path.join(dataset_folder, location)
            msg = f"Could not find xml for {storage} at {path}"
            assert_true(os.path.exists(path), msg)
        if 'tableRootLocation' in metadata:
            table_folder = os.path.join(dataset_folder, metadata['tableRootLocation'])
            msg = f"Could not find table root folder at {table_folder}"
            assert_true(os.path.isdir(table_folder), msg)
            default_table = os.path.join(table_folder, 'default.tsv')
            msg = f"Could not find default table at {default_table}"
            assert_true(os.path.exists(default_table), msg)

    # dynamic validation of view metadata
    view = metadata['view']
    validate_view_metadata(view, sources=[name], assert_true=assert_true)


def validate_view_metadata(view, sources=None, assert_true=_assert_true):
    # TODO static validation with json schema

    # dynamic validation of sources
    all_display_sources = []
    displays = view["sourceDisplays"]
    for display in displays:
        display_metadata = list(display.values())[0]
        display_name = display_metadata['name']
        display_sources = display_metadata["sources"]
        all_display_sources.extend(display_sources)
        if sources is not None:
            wrong_sources = list(set(display_sources) - set(sources))
            msg = f"Found wrong sources {wrong_sources} in sourceDisplay {display_name}"
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
