import numpy as np
import pandas as pd


def _get_n_positions(sources, positions):
    if positions is None:
        return len(sources)
    else:
        # we assume row major indexing
        stride = np.max([pos[1] for pos in positions])
        grid_ids = [stride * pos[0] + pos[1] for pos in positions]
        return int(np.max(grid_ids)) + 1


def compute_grid_view_table(sources, table_path, positions=None, **additional_columns):
    n_positions = _get_n_positions(sources, positions)
    if additional_columns:
        assert all(len(val) == n_positions for val in additional_columns.values())
        data = [[i] + [val[i] for val in additional_columns.values()]
                for i in range(n_positions)]
    else:
        data = [[i] for i in range(n_positions)]
    columns = ['grid_id'] + list(additional_columns.keys())

    table = pd.DataFrame(data, columns=columns)
    table.to_csv(table_path, sep='\t', index=False)


def check_grid_view_table(sources, table_path, positions=None):
    table = pd.read_csv(table_path, sep='\t')
    n_positions = _get_n_positions(sources, positions)
    if 'grid_id' not in table.columns:
        raise ValueError("Expect grid view table to have a 'grid_id' column")
    if table.shape[0] != n_positions:
        msg = f"Expect number of rows in the table to be the same as the number of grid postions: {len(sources)}"
        raise ValueError(msg)
