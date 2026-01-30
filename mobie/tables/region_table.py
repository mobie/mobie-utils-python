"""Functionality for creating region display tables.
"""
import os
from typing import Dict, List, Union

import pandas as pd
from .utils import read_table


def compute_region_table(
    sources: Union[List, Dict],
    table_path: str,
    **additional_columns
) -> None:
    """Create the table for a region display.

    Args:
        sources: The image, segmentation or spot sources in the the region display.
        table_path: The path for saving the region table.
        additional_columns: Additonal columns to add to the region table.
    """
    first_col_name = "region_id"

    if isinstance(sources, list):
        sources = {ii: source for ii, source in enumerate(sources)}

    if additional_columns:
        # this case is a bit more complicated, not implemented for now
        assert all(len(val) == len(sources) for val in additional_columns.values())
        data = [[region_id] + [val[i] for val in additional_columns.values()]
                for i, region_id in enumerate(sources.keys())]
        columns = [first_col_name] + list(additional_columns.keys())
    else:
        data = [[region_id, "-".join(source)] for region_id, source in sources.items()]
        columns = [first_col_name, "source"]

    table = pd.DataFrame(data, columns=columns)
    os.makedirs(os.path.split(table_path)[0], exist_ok=True)
    table.to_csv(table_path, sep="\t", index=False, na_rep="nan")


def check_region_table(sources: Union[List, Dict], table_path: str) -> None:
    """Check if the given table path contains a valid region table.

    Raises a ValueError if the table is not valid.

    Args:
        sources: The expected sources for the region display.
        table_path: The path to the table to check.
    """
    first_col_name = "region_id"
    table = read_table(table_path)

    if first_col_name not in table.columns:
        raise ValueError(f"Expect grid view table to have a '{first_col_name}' column")

    # check that keys of sources are a subset of the annotation ids in the table
    source_ids = set(range(len(list))) if isinstance(sources, list) else set(sources.keys())
    table_ids = set(table[first_col_name])
    missing_ids = list(source_ids - table_ids)

    if missing_ids:
        msg = f"The annotation ids {missing_ids} are missing from the table."
        raise ValueError(msg)

    if table.shape[1] == 1:
        msg = "Expect grid view table to have at least two columns"
        raise ValueError(msg)
