import json
import os
from glob import glob

import pandas as pd

from ...tables.util import remove_background_label_row


# TODO
def migrate_source_metadata(item):
    pass


def migrate_sources_metadata(folder):
    in_file = os.path.join(folder, 'images', 'images.json')
    assert os.path.exists(in_file), in_file
    with open(in_file, 'r') as f:
        sources_in = json.load(f)

    sources_out = {}
    for name, item in sources_in.item():
        sources_out[name] = migrate_source_metadata(item)

    out_file = os.path.join(folder, 'sources.json')
    with open(out_file, 'w') as f:
        json.dump(sources_out, f, indent=2, sort_keys=True)

    os.remove(in_file)


def migrate_table(table_path):
    table = pd.read_csv(table_path, sep='\t')
    table = remove_background_label_row(table)
    out_path = table_path.replace('.csv', '.tsv')
    table.to_csv(out_path, sep='\t', index=False)
    os.remove(table_path)


def migrate_tables(folder):
    table_root = os.path.join(folder, 'tables')
    assert os.path.exists(table_root)
    table_names = os.listdir(table_root)
    for table_name in table_names:
        table_folder = os.path.join(table_root, table_name)
        table_paths = glob(os.path.join(table_folder, "*.csv"))
        for table_path in table_paths:
            migrate_table(table_path)


def migrate_dataset(folder):
    migrate_sources_metadata(folder)
    migrate_tables(folder)
