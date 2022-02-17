import os
from glob import glob

import numpy as np
import pandas as pd
from elf.io import open_file
from jsonschema import ValidationError
from pybdv.metadata import get_name
from .utils import _assert_true, _assert_equal, validate_with_schema


def is_default_table(table):
    default_columns_2d = {"label_id",
                          "anchor_x", "anchor_y",
                          "bb_min_x", "bb_min_y",
                          "bb_max_x", "bb_max_y"}
    default_columns_3d = {"label_id",
                          "anchor_x", "anchor_y", "anchor_z",
                          "bb_min_x", "bb_min_y", "bb_min_z",
                          "bb_max_x", "bb_max_y", "bb_max_z"}
    is_default_3d = len(default_columns_3d - set(table.columns)) == 0
    if is_default_3d:
        return True
    is_default_2d = len(default_columns_2d - set(table.columns)) == 0
    return is_default_2d


def check_table(table, ref_label_ids):
    if "label_id" not in table.columns:
        return False
    this_label_ids = set(table["label_id"].values)
    return len(this_label_ids - ref_label_ids) == 0


def _load_table(table_path):
    return pd.read_csv(table_path, sep="\t" if os.path.splitext(table_path)[1] == ".tsv" else ",")


def check_tables(table_folder, assert_true):
    msg = f"Could not find table root folder at {table_folder}"
    assert_true(os.path.isdir(table_folder), msg)

    all_tables = glob(os.path.join(table_folder, "*.tsv")) + glob(os.path.join(table_folder, "*.csv"))
    ref_label_ids = None
    for table_path in all_tables:
        table = _load_table(table_path)
        if is_default_table(table):
            ref_label_ids = set(table["label_id"].values)
            break

    msg = f"Could not find default table in {table_folder}"
    assert_true(ref_label_ids is not None, msg)
    for table_path in all_tables:
        table = _load_table(table_path)
        if is_default_table(table):
            continue
        msg = f"The table {table_path} contains invalid label_ids"
        assert_true(check_table(table, ref_label_ids), msg)


def validate_source_metadata(name, metadata, dataset_folder=None,
                             assert_true=_assert_true, assert_equal=_assert_equal):
    # static validation with json schema
    try:
        validate_with_schema(metadata, "source")
    except ValidationError as e:
        msg = f"{e}"
        assert_true(False, msg)

    source_type = list(metadata.keys())[0]
    metadata = metadata[source_type]
    # dynamic validation of paths
    if dataset_folder is not None:
        for format_, storage in metadata["imageData"].items():
            path = storage.get("relativePath", None)
            if path is None:
                continue
            path = os.path.join(dataset_folder, storage["relativePath"])
            msg = f"Could not find data for {name} at {path}"
            assert_true(os.path.exists(path), msg)

            # check that the source name and name in the xml agree for bdv formats
            if format_.startswith("bdv"):
                bdv_name = get_name(path, setup_id=0)
                msg = f"{path}: Source name and name in bdv metadata disagree: {name} != {bdv_name}"
                assert_equal(name, bdv_name, msg)

            # check that the source name and name in the ome.zarr metadata agree
            if format_ == "ome.zarr":
                with open_file(path, "r") as f:
                    ome_name = f.attrs["multiscales"][0]["name"]
                assert_equal(name, ome_name)

        if "tableData" in metadata:
            table_folder = os.path.join(dataset_folder, metadata["tableData"]["tsv"]["relativePath"])
            check_tables(table_folder, assert_true)


def check_annotation_tables(table_folder, tables, assert_true):
    ref_grid_ids = None
    for table_name in tables:
        table_path = os.path.join(table_folder, table_name)
        msg = f"Table {table_path} does not exist."
        assert_true(os.path.exists(table_path), msg)

        table = _load_table(table_path)
        msg = f"Table {table_path} does not contain the 'annotation_id' column"
        assert_true("annotation_id" in table.columns, msg)

        n_cols = table.shape[1]
        msg = f"Table {table_path} contains only a single column"
        assert_true(n_cols > 1, msg)

        this_grid_ids = table["annotation_id"].values
        if ref_grid_ids is None:
            ref_grid_ids = this_grid_ids
        else:
            msg = f"The grid ids for the table {table_path} are inconsistent with the grid ids in other tables"
            assert_true(np.array_equal(ref_grid_ids, this_grid_ids), msg)


def validate_view_metadata(view, sources=None, dataset_folder=None, assert_true=_assert_true):
    # static validation with json schema
    try:
        validate_with_schema(view, "view")
    except ValidationError as e:
        msg = f"{e}"
        assert_true(False, msg)

    displays = view.get("sourceDisplays")
    # dynamic validation of valid sources if sources argument is passed
    if sources is not None:
        valid_sources = set(sources)

        # validate source trafos
        source_transformations = view.get("sourceTransformations")
        if source_transformations is not None:
            for transform in source_transformations:
                transform_metadata = list(transform.values())[0]

                # validate the sources for this source transform
                transform_sources = transform_metadata["sources"]
                wrong_sources = list(set(transform_sources) - valid_sources)
                msg = f"Found wrong sources {wrong_sources} in source transform"
                assert_true(len(wrong_sources) == 0, msg)

                # extend the valid sources if we add source names with this trafo
                if "sourceNamesAfterTransform" in transform_metadata:
                    new_source_names = transform_metadata["sourceNamesAfterTransform"]
                    if isinstance(new_source_names, dict):
                        new_source_names = [source for v in new_source_names.values() for source in v]
                    valid_sources = valid_sources.union(set(new_source_names))

                if "mergedGridSourceName" in transform_metadata:
                    new_source_names = {transform_metadata["mergedGridSourceName"]}
                    valid_sources = valid_sources.union(set(new_source_names))

        # validate source displays
        if displays is not None:
            for display in displays:
                display_metadata = list(display.values())[0]
                display_sources = display_metadata["sources"]
                if isinstance(display_sources, dict):
                    display_sources = [source for this_sources in display_sources.values() for source in this_sources]
                wrong_sources = list(set(display_sources) - valid_sources)
                msg = f"Found wrong sources {wrong_sources} in sourceDisplay"
                assert_true(len(wrong_sources) == 0, msg)

    # dynamic validation of annotation tables
    if displays is not None and dataset_folder is not None:
        for display in displays:
            display_type = list(display.keys())[0]
            if display_type == "sourceAnnotationDisplay":
                display_metadata = list(display.values())[0]
                table_folder = os.path.join(dataset_folder, display_metadata["tableData"]["tsv"]["relativePath"])
                tables = display_metadata.get("tables")
                if tables is not None:
                    check_annotation_tables(table_folder, tables, assert_true)
