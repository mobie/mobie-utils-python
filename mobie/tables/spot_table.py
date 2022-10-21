import os
import warnings

import numpy as np

from .utils import read_table


def _process_additional_spot_table(input_table, table_out_path, spot_ids):
    input_table = read_table(input_table)
    if "spot_id" in input_table.columns:
        this_spot_ids = input_table["spot_id"].values
        # make sure there are no extra spot ids (missing ones are fine)
        assert len(set(this_spot_ids.tolist()) - set(spot_ids.tolist())) == 0
    else:
        assert len(input_table) == len(spot_ids)
        warnings.warn("Extra spot table was missing the 'spot_id' column. Same spot ids as in the main table are used.")
        input_table["spot_id"] = spot_ids

    input_table.to_csv(table_out_path, sep="\t", index=False, na_rep="nan")


def process_spot_table(table_folder, input_table, is_2d, additional_tables=None, float_precision="%.4f"):
    os.makedirs(table_folder, exist_ok=True)

    # process the input table
    input_table = read_table(input_table)
    if "spot_id" in input_table.columns:
        spot_ids = input_table["spot_id"].values
    else:
        warnings.warn("Spot table did not contain a 'spot_id' column. Will add naive column based on row index.")
        spot_ids = np.arange(1, len(input_table) + 1).astype("uint64")
        input_table["spot_id"] = spot_ids

    coordinate_columns = ["x", "y"] if is_2d else ["x", "y", "z"]
    assert all(col in input_table.columns for col in coordinate_columns)
    for col in coordinate_columns:
        input_table[col] = input_table[col].astype("float64")

    table_out_path = os.path.join(table_folder, "default.tsv")
    input_table.to_csv(table_out_path, sep="\t", index=False, na_rep="nan", float_format=float_precision)

    if additional_tables:
        for name, table in additional_tables.items():
            table_out_path = os.path.join(table_folder, name if name.endswith(".tsv") else f"{name}.tsv")
            _process_additional_spot_table(table, table_out_path, spot_ids)
