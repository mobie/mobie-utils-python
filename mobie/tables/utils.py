import os
import pandas as pd


def remove_background_label_row(table):
    if table["label_id"].values[0] == 0:
        table = table.drop(axis="index", labels=0)
    return table


# tables can either be passed as filepath or as pandas DataFrame (in this case they are just returned)
# this can later be extended to support tables in other data formats (ome.zarr)
def read_table(table):
    if isinstance(table, pd.DataFrame):
        return table
    # support reading tables in csv and tsv format
    elif isinstance(table, str):
        if not os.path.exists(table):
            raise ValueError(f"Table {table} does not exist.")
        return pd.read_csv(table, sep="\t" if os.path.splitext(table)[1] == ".tsv" else ",")
    else:
        raise ValueError(f"Invalid table format, expected either filepath or pandas DataFrame, got {type(table)}.")
