import numpy as np
import pandas as pd


def _get_grid_to_sources(sources, positions):
    if positions is None:
        return {ii: source for ii, source in enumerate(sources)}
    else:
        assert len(sources) == len(positions), f"{len(sources)}, {len(positions)}"
        # we assume row major indexing
        stride = np.max([pos[1] for pos in positions])
        grid_ids = [stride * pos[0] + pos[1] for pos in positions]
        grid_to_source = {
            grid_id: source for grid_id, source in zip(grid_ids, sources)
        }
        return grid_to_source


def compute_grid_view_table(sources, table_path, positions=None, **additional_columns):
    first_col_name = 'annotation_id'
    grid_to_source = _get_grid_to_sources(sources, positions)

    if additional_columns:
        # this case is a bit more complicated, not implemented for now
        assert all(len(val) == len(grid_to_source) for val in additional_columns.values())
        data = [[i] + [val[i] for val in additional_columns.values()]
                for i in range(grid_to_source)]
        columns = [first_col_name] + list(additional_columns.keys())
    else:
        data = [[i, source] for i, source in grid_to_source.items()]
        columns = [first_col_name, 'source']

    table = pd.DataFrame(data, columns=columns)
    table.to_csv(table_path, sep='\t', index=False)


def check_grid_view_table(sources, table_path, positions=None):
    first_col_name = 'annotation_id'
    table = pd.read_csv(table_path, sep='\t')
    grid_to_source = _get_grid_to_sources(sources, positions)
    if first_col_name not in table.columns:
        raise ValueError(f"Expect grid view table to have a '{first_col_name}' column")
    if table.shape[0] != len(grid_to_source):
        msg = f"Expect number of rows in the table to be the same as the number of grid postions: {len(sources)}"
        raise ValueError(msg)
    if table.shape[1] == 1:
        msg = "Expect grid view table to have at least two columns"
        raise ValueError(msg)
