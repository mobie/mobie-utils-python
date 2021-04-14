import json
import os
from .migrate_dataset import migrate_dataset
from ...metadata import write_project_metadata


def migrate_project(root, parse_menu_name=None, parse_source_name=None):
    ds_file = os.path.join(root, 'datasets.json')
    with open(ds_file, 'r') as f:
        metadata = json.load(f)
    ds_list = metadata['datasets']

    for ds in ds_list:
        ds_folder = os.path.join(root, ds)
        assert os.path.exists(ds_folder), ds_folder
        print("Migrate dataset:", ds)
        migrate_dataset(ds_folder, parse_menu_name=parse_menu_name,
                        parse_source_name=parse_source_name)

    metadata['specVersion'] = '0.2.0'
    write_project_metadata(root, metadata)
    os.remove(ds_file)
