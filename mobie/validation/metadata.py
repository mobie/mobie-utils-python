import os

# from elf.io import open_file
from jsonschema import ValidationError
from pybdv.metadata import get_name, get_data_path

from .tables import check_region_tables, check_segmentation_tables, check_spot_tables, check_tables_in_view
from .utils import _assert_true, _assert_equal, _assert_in, validate_with_schema, load_json_from_s3
from ..xml_utils import parse_s3_xml


def _check_bdv_n5_s3(xml, assert_true):
    path_in_bucket, server, bucket, _ = parse_s3_xml(xml)
    address = os.path.join(server, bucket, path_in_bucket, "attributes.json")
    try:
        attrs = load_json_from_s3(address)
    except Exception:
        assert_true(False, f"Can't find bdv.n5.s3 file at {address}")
    assert_true("n5" in attrs, "Invalid n5 file at {address}")


def _check_ome_zarr_s3(address, name, assert_true, assert_equal, channel):
    try:
        attrs = load_json_from_s3(os.path.join(address, ".zattrs"))
    except Exception:
        assert_true(False, f"Can't find ome.zarr..s3file at {address}")
    # we can't do this check if we only load a sub-channel
    if channel is None:
        ome_name = attrs["multiscales"][0]["name"]
        assert_equal(name, ome_name, f"Source name and name in ngff metadata don't match: {name} != {ome_name}")


def _check_data(storage, format_, name, dataset_folder,
                require_local_data, require_remote_data,
                assert_true, assert_equal):
    # checks for bdv format
    if format_.startswith("bdv"):
        path = os.path.join(dataset_folder, storage["relativePath"])
        assert_true(os.path.exists(path), f"Could not find data for {name} at {path}")

        # check that the source name and name in the xml agree for bdv formats
        bdv_name = get_name(path, setup_id=0)
        msg = f"{path}: Source name and name in bdv metadata disagree: {name} != {bdv_name}"
        assert_equal(name, bdv_name, msg)

        # check that the remote s3 address exists
        if format_.endswith(".s3") and require_remote_data:
            _check_bdv_n5_s3(path, assert_true)

        # check that the referenced local file path exists
        elif require_local_data:
            data_path = get_data_path(path, return_absolute_path=True)
            assert_true(os.path.exists(data_path), f"Can't find local data @ {data_path}")

    # local ome.zarr check: source name and name in the ome.zarr metadata agree
    elif format_ == "ome.zarr" and require_local_data:
        path = os.path.join(dataset_folder, storage["relativePath"])
        assert_true(os.path.exists(path), f"Could not find data for {name} at {path}")

        # we disable the name check for the time being since it seems to not be necessary,
        # AND restricting the name in this fashion prevents embedding existing ome.zarr files in mobie projects
        # with open_file(path, "r", ext=".zarr") as f:
        #     ome_name = f.attrs["multiscales"][0]["name"]
        # # we can't do this check if we only load a sub-channel
        # if "channel" not in storage:
        #     assert_equal(name, ome_name, f"Source name and name in ngff metadata don't match: {name} != {ome_name}")

    # remote ome.zarr check:
    elif format_ == "ome.zarr.s3" and require_remote_data:
        s3_address = storage["s3Address"]
        channel = storage.get("channel")
        _check_ome_zarr_s3(s3_address, name, assert_true, assert_equal, channel)


def validate_source_metadata(name, metadata,
                             dataset_folder=None, is_2d=None,
                             require_local_data=True, require_remote_data=False,
                             assert_true=_assert_true, assert_equal=_assert_equal,
                             assert_in=_assert_in):
    # static validation with json schema
    try:
        validate_with_schema(metadata, "source")
    except ValidationError as e:
        msg = f"{e}"
        assert_true(False, msg)

    source_type, source_metadata = next(iter(metadata.items()))
    # dynamic validation of paths / remote addresses
    if dataset_folder is not None:
        if "imageData" in source_metadata:
            for format_, storage in source_metadata["imageData"].items():
                _check_data(storage, format_, name, dataset_folder,
                            require_local_data, require_remote_data,
                            assert_true, assert_equal)

        if "tableData" in source_metadata:
            table_folder = os.path.join(dataset_folder, source_metadata["tableData"]["tsv"]["relativePath"])
            if source_type == "segmentation":
                assert is_2d is not None
                check_segmentation_tables(table_folder, is_2d, assert_true=assert_true)
            elif source_type == "regions":
                check_region_tables(table_folder, assert_true=assert_true)
            elif source_type == "spots":
                assert is_2d is not None
                check_spot_tables(table_folder, is_2d, assert_true=assert_true)


# all view validation that need a list of sources
def _dynamic_view_source_validation(view, sources, displays, assert_true):
    valid_sources = set(sources)

    # validate source trafos
    source_transformations = view.get("sourceTransforms")
    if source_transformations is not None:

        for transform in source_transformations:
            transform_type, transform_metadata = next(iter(transform.items()))

            # validate the sources for this source transform
            assert_true(
                "sources" in transform_metadata or "nestedSources" in transform_metadata,
                "Need either 'sources' or 'nestedSources' in transform metadata"
            )
            if "sources" in transform_metadata:
                transform_sources = transform_metadata["sources"]
            else:
                transform_sources = transform_metadata["nestedSources"]
                transform_sources = [src for srcs in transform_sources for src in srcs]

            wrong_sources = list(set(transform_sources) - valid_sources)
            msg = f"Found wrong sources {wrong_sources} in source transform"
            assert_true(len(wrong_sources) == 0, msg)

            # extend the valid sources if we add source names with this trafo
            if "sourceNamesAfterTransform" in transform_metadata:
                new_source_names = transform_metadata["sourceNamesAfterTransform"]
                if isinstance(new_source_names, dict):
                    new_source_names = [source for v in new_source_names.values() for source in v]
                valid_sources = valid_sources.union(set(new_source_names))

            # for a merged grid we extend the sources by the merged grid itself, and also
            # add new sources (the individual sources after transformation) that get the grid name as suffix
            if transform_type == "mergedGrid":
                grid_name = transform_metadata["mergedGridSourceName"]
                valid_sources = valid_sources.union({grid_name})
                valid_sources = valid_sources.union({f"{source}_{grid_name}" for source in transform_sources})
                # if we have a metadata source check that it is included in our actual sources
                if "metadataSource" in transform_metadata:
                    metadata_source = transform_metadata["metadataSource"]
                    assert_true(metadata_source in valid_sources,
                                f"Cannot find metadata source {metadata_source} in the list of sources.")
                    # advanced checks: could check that the dtype, shape etc. agree for all sources,
                    # see https://github.com/mobie/covid-if-project/issues/11#issue-1393850820
                    # (but this would make the checks more complicated and they would potentially take much longer)

    # validate source displays
    if displays is not None:
        for display in displays:
            display_metadata = next(iter(display.values()))
            display_sources = display_metadata["sources"]
            if isinstance(display_sources, dict):
                display_sources = [source for this_sources in display_sources.values() for source in this_sources]
            wrong_sources = list(set(display_sources) - valid_sources)
            msg = f"Found wrong sources {wrong_sources} in sourceDisplay: {display_metadata['name']}"
            assert_true(len(wrong_sources) == 0, msg)


# all view validations that need dataset metadata and the dataset folder
# (which currently is validating the tables)
def _dynamic_view_table_validation(displays, dataset_folder, dataset_metadata, assert_true):
    all_sources = dataset_metadata["sources"]

    for display in displays:
        display_type, display_metadata = next(iter(display.items()))

        if display_type == "regionDisplay":
            table_source = display_metadata["tableSource"]
            additional_tables = display_metadata.get("additionalTables", None)
            color_by_col = display_metadata.get("colorByColumn", None)
            check_tables_in_view(
                all_sources, table_source, dataset_folder,
                merge_columns=["region_id", "timepoint"],
                additional_tables=additional_tables,
                expected_columns=None if color_by_col is None else [color_by_col], assert_true=assert_true,
            )

        elif display_type == "spotDisplay":
            display_sources = display_metadata["sources"]
            additional_tables = display_metadata.get("additionalTables", None)
            color_by_col = display_metadata.get("colorByColumn", None)
            for source in display_sources:
                check_tables_in_view(
                    all_sources, source, dataset_folder,
                    merge_columns=["spot_id", "timepoint"],
                    additional_tables=additional_tables,
                    expected_columns=None if color_by_col is None else [color_by_col], assert_true=assert_true,
                )

        elif display_type == "segmentationDisplay":

            # for the segmentation display we only need to check the tables if at least one of the
            # following display_metadata fields is present:
            additional_tables = display_metadata.get("additionalTables", None)
            color_by_col = display_metadata.get("colorByColumn", None)
            show_tables = display_metadata.get("showTable", False)

            if any((color_by_col, additional_tables, show_tables)):
                display_sources = display_metadata["sources"]
                for source in display_sources:
                    check_tables_in_view(
                        all_sources, source, dataset_folder,
                        merge_columns=["label_id", "timepoint"],
                        additional_tables=additional_tables,
                        expected_columns=None if color_by_col is None else [color_by_col], assert_true=assert_true,
                    )


def _dynamic_view_display_validation(displays, dataset_folder, dataset_metadata, assert_true):
    sources = dataset_metadata["sources"]

    display_to_source_type = {"spotDisplay": "spots", "imageDisplay": "image", "segmentationDisplay": "segmentation"}

    for display in displays:
        display_type, display_data = next(iter(display.items()))

        # nothing to check for region displays
        if display_type == "regionDisplay":
            continue

        this_sources = display_data["sources"]
        if isinstance(this_sources, dict):
            this_sources = [source for this_sources in this_sources.values() for source in this_sources]
        # we may have transient sources that are created in the view and which are not part of the sources listed
        # in the dataset metadata. We can't check those for correctness, so we skip them here.
        this_sources = [source for source in this_sources if source in sources]

        expected_source_type = display_to_source_type[display_type]
        assert_true(
            all((next(iter(sources[name])) == expected_source_type for name in this_sources)),
            f"Not all sources are of the expected type {expected_source_type} for a {display_type}"
        )

        # for segmentation displays: make sure that either all or none of the sources have table data
        if display_type == "segmentationDisplay":
            have_table = ["tableData" in next(iter(sources[name].values())) for name in this_sources]
            if any(have_table):
                assert_true(
                    all(have_table), "Either all or none of the sources in a segmentation display need to have tables"
                )


def validate_view_metadata(view, sources=None, dataset_folder=None, assert_true=_assert_true, dataset_metadata=None):
    # static validation with json schema
    try:
        validate_with_schema(view, "view")
    except ValidationError as e:
        msg = f"{e}"
        assert_true(False, msg)

    displays = view.get("sourceDisplays")

    # dynamic validation of views that does not need any further metadata
    # (currently: check that we have value limits for the luts that need them)
    if displays is not None:
        numeric_luts = ("viridis", "blueWhiteRed")
        for display in displays:
            display_metadata = next(iter(display.values()))
            lut = display_metadata.get("lut", None)
            if lut is not None and lut in numeric_luts:
                assert_true("valueLimits" in display_metadata)

    # dynamic validation: check that all sources in displays and source transforms are valid
    # can only be checkeed if the sources argument is passed
    if sources is not None:
        _dynamic_view_source_validation(view, sources, displays, assert_true)

    # dynamic validation: check that the sourceDisplays are correct.
    # can only be checked if the dataset_metadata nd dataset_folder are passed
    if displays is not None and dataset_metadata is not None:
        assert dataset_folder is not None
        _dynamic_view_display_validation(displays, dataset_folder, dataset_metadata, assert_true)
        _dynamic_view_table_validation(displays, dataset_folder, dataset_metadata, assert_true)
