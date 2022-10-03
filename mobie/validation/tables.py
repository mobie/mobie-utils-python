import os
from glob import glob

import pandas as pd

from .utils import _assert_true


# TODO refactor into upstream utils, use where we load external tables to support tsv or csv
def _load_table(table_path):
    return pd.read_csv(table_path, sep="\t" if os.path.splitext(table_path)[1] == ".tsv" else ",")


def _check_tables(table_folder, default_table_columns, merge_columns, assert_true):
    # check that table folder and default table exist
    assert_true(os.path.isdir(table_folder), f"Table root folder {table_folder} does not exist.")
    default_table_path = os.path.join(table_folder, "default.tsv")
    assert_true(os.path.exists(default_table_path), f"Default table {default_table_path} does not exist.")

    # check that the default table contains all the expected columns
    default_table = _load_table(default_table_path)
    assert_true(default_table.shape[1] > 1, f"Default table {default_table_path} contains only a single column")
    for col in default_table_columns:
        assert_true(
            col in default_table.columns,
            f"Expected column {col} is not present in the default table @ {default_table_path}."
        )

    # get all expected merge columns and their values
    expected_merge_columns = {}
    for col in merge_columns:
        if col in default_table.columns:
            expected_merge_columns[col] = set(default_table[col].values)
    # we always have at least one of the merge columns, so this is a normal assert
    # because it can only be triggered by an internal error
    assert expected_merge_columns

    # check the additional tables
    additional_tables = list(
        set(
            glob(os.path.join(table_folder, "*.tsv")) + glob(os.path.join(table_folder, "*.csv"))
        ) - {default_table_path}
    )
    for table_path in additional_tables:
        table = _load_table(table_path)
        assert_true(table.shape[1] > 1, f"Table {table_path} contains only a single column")

        # check that the merge columns are present
        # and that we don't have any ids in them that are not in the default table
        for col, ref_values in expected_merge_columns.items():
            assert_true(
                col in table, f"Expected column {col} is not present in additional table @ {table_path}"
            )
            this_values = set(table[col].values)
            assert_true(
                len(this_values - ref_values) == 0, f"Unexpected ids in column {col} in additional table @ {table_path}"
            )


def check_region_tables(table_folder, assert_true=_assert_true):
    default_columns = ["region_id"]
    merge_columns = ["region_id", "timepoint"]
    _check_tables(table_folder, default_columns, merge_columns, assert_true=assert_true)


def check_segmentation_tables(table_folder, is_2d, assert_true=_assert_true):
    default_columns = [
        "label_id",  "anchor_x", "anchor_y",
        "bb_min_x", "bb_min_y", "bb_max_x", "bb_max_y"
    ]
    if not is_2d:
        default_columns.extend(["anchor_z", "bb_min_z", "bb_max_z"])
    merge_columns = ["label_id", "timepoint"]
    _check_tables(table_folder, default_columns, merge_columns, assert_true=assert_true)


def check_spot_tables(table_folder, is_2d, assert_true=_assert_true):
    default_columns = ["spot_id", "x", "y"]
    if not is_2d:
        default_columns.append("z")
    merge_columns = ["region_id", "timepoint"]
    _check_tables(table_folder, default_columns, merge_columns, assert_true=assert_true)


def check_tables_in_view(
    sources, table_source, dataset_folder, additional_tables=None, expected_columns=None, assert_true=_assert_true
):
    assert_true(table_source in sources, f"The table source {table_source} is not present in the source metadata.")

    source_metadata = next(sources[table_source].values())
    assert_true("tableData" in source_metadata, f"Source {table_source} does not contain tableData.")
    table_folder = os.path.join(dataset_folder, source_metadata["tableData"]["tsv"]["relativePath"])

    # check that all additional tables that were specified (if any) exist
    if additional_tables is not None:
        for table in additional_tables:
            assert_true(
                os.path.exists(os.path.exists(dataset_folder, table)),
                f"Could not find additional table {table} in {dataset_folder}"
            )

    # check that expected columns are in the loaded tables
    if expected_columns is not None:

        tables = [_load_table(os.path.join(table_folder, "default.tsv"))]
        if additional_tables is not None:
            for table in additional_tables:
                tables.append(
                    _load_table(os.path.join(table_folder, table))
                )

        for col in expected_columns:
            have_expected_col = False
            for table in tables:
                have_expected_col = col in table.columns
                if have_expected_col:
                    break
            assert_true(have_expected_col, f"Could not find the expected column {col} in any of the tables in the view")