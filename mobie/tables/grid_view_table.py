import pandas as pd


def compute_grid_view_table(sources, table_path, **additional_columns):
    first_col_name = "annotation_id"

    if isinstance(sources, list):
        sources = {ii: source for ii, source in enumerate(sources)}

    if additional_columns:
        # this case is a bit more complicated, not implemented for now
        assert all(len(val) == len(sources) for val in additional_columns.values())
        data = [[annotation_id] + [val[i] for val in additional_columns.values()]
                for i, annotation_id in enumerate(sources.keys())]
        columns = [first_col_name] + list(additional_columns.keys())
    else:
        data = [[annotation_id, "-".join(source)] for annotation_id, source in sources.items()]
        columns = [first_col_name, "source"]

    table = pd.DataFrame(data, columns=columns)
    table.to_csv(table_path, sep="\t", index=False, na_rep="nan")


def check_grid_view_table(sources, table_path):
    first_col_name = "annotation_id"
    table = pd.read_csv(table_path, sep="\t")

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
