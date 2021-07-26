import os
import pandas as pd
from glob import glob

import numpy as np
from jsonschema import ValidationError
from pybdv.metadata import get_name
from .utils import _assert_true, _assert_equal, validate_with_schema


def is_default_table(table):
    default_table_columns = {'label_id',
                             'anchor_x', 'anchor_y', 'anchor_z',
                             'bb_min_x', 'bb_min_y', 'bb_min_z',
                             'bb_max_x', 'bb_max_y', 'bb_max_z'}
    return len(default_table_columns - set(table.columns)) == 0


def check_table(table, ref_label_ids):
    if 'label_id' not in table.columns:
        return False
    this_label_ids = set(table['label_id'].values)
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
            ref_label_ids = set(table['label_id'].values)
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
        validate_with_schema(metadata, 'source')
    except ValidationError as e:
        msg = f"{e}"
        assert_true(False, msg)

    source_type = list(metadata.keys())[0]
    metadata = metadata[source_type]
    # dynamic validation of paths
    if dataset_folder is not None:
        for format_, storage in metadata['imageData'].items():
            path = storage.get('relativePath', None)
            if path is None:
                continue
            path = os.path.join(dataset_folder, storage['relativePath'])
            msg = f"Could not find data for {name} at {path}"
            assert_true(os.path.exists(path), msg)
            # check that source name and name in the xml agree for bdv formats
            if path.endswith('.xml'):
                bdv_name = get_name(path, setup_id=0)
                assert_equal(name, bdv_name)
        if 'tableData' in metadata:
            table_folder = os.path.join(dataset_folder, metadata['tableData']['tsv']['relativePath'])
            check_tables(table_folder, assert_true)


def check_grid_tables(table_folder, tables, assert_true):
    ref_grid_ids = None
    for table_name in tables:
        table_path = os.path.join(table_folder, table_name)
        msg = f"Table {table_path} does not exist."
        assert_true(os.path.exists(table_path), msg)

        table = _load_table(table_path)
        msg = f"Table {table_path} does not contain the grid_id column"
        assert_true('grid_id' in table.columns, msg)

        n_cols = table.shape[1]
        msg = f"Table {table_path} contains only a single column"
        assert_true(n_cols > 1, msg)

        this_grid_ids = table['grid_id'].values
        if ref_grid_ids is None:
            ref_grid_ids = this_grid_ids
        else:
            msg = f"The grid ids for the table {table_path} are inconsistent with the grid ids in other tables"
            assert_true(np.array_equal(ref_grid_ids, this_grid_ids), msg)


def validate_view_metadata(view, sources=None, dataset_folder=None, assert_true=_assert_true):
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
            if isinstance(display_sources, dict):
                display_sources = [source for this_sources in display_sources.values() for source in this_sources]
            all_display_sources.extend(display_sources)
            if sources is not None:
                wrong_sources = list(set(display_sources) - set(sources))
                msg = f"Found wrong sources {wrong_sources} in sourceDisplay; expected one of {sources}"
                assert_true(len(wrong_sources) == 0, msg)

    source_transformations = view.get("sourceTransformations")
    if source_transformations is not None:
        all_display_sources = set(all_display_sources)
        for transform in source_transformations:
            transform_type = list(transform.keys())[0]
            transform_metadata = list(transform.values())[0]
            transform_name = transform_metadata['name']

            # validate the sources for this source transform
            transform_sources = transform_metadata["sources"]
            wrong_sources = list(set(transform_sources) - all_display_sources)
            msg = f"Found wrong sources {wrong_sources} in transform {transform_name}"
            assert_true(len(wrong_sources) == 0, msg)

            # validate the grid tables for a grid transform
            # (only possible if we have the dataset folder)
            if transform_type == "grid" and dataset_folder is not None:
                table_folder = os.path.join(dataset_folder, transform_metadata["tableData"]["tsv"]["relativePath"])
                tables = transform_metadata["tables"]
                check_grid_tables(table_folder, tables, assert_true)
