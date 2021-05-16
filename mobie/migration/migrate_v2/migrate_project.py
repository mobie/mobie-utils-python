import json
import os
from .migrate_dataset import migrate_dataset
from .migrate_view_spec import migrate_view_spec
from ...metadata import write_project_metadata


def _migrate_project(root, ds_list, metadata, ds_file,
                     parse_source_name, parse_menu_name):
    for ds in ds_list:
        ds_folder = os.path.join(root, ds)
        assert os.path.exists(ds_folder), ds_folder
        print("Migrate dataset:", ds)
        migrate_dataset(ds_folder, parse_menu_name=parse_menu_name,
                        parse_source_name=parse_source_name)

    metadata['specVersion'] = '0.2.0'
    write_project_metadata(root, metadata)
    os.remove(ds_file)


def _update_view_spec(root, ds_list):
    for ds in ds_list:
        ds_folder = os.path.join(root, ds)
        assert os.path.exists(ds_folder), ds_folder
        migrate_view_spec(ds_folder)


def migrate_project(root, parse_menu_name=None, parse_source_name=None, update_view_spec=False):
    ds_file = os.path.join(root, 'project.json') if update_view_spec else os.path.join(root, 'datasets.json')
    with open(ds_file, 'r') as f:
        metadata = json.load(f)
    ds_list = metadata['datasets']

    if update_view_spec:
        _update_view_spec(root, ds_list)
    else:
        _migrate_project(root, ds_list, metadata, ds_file,
                         parse_source_name, parse_menu_name)
