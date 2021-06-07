import json
import os
from .migrate_dataset import migrate_dataset
from .migrate_data_spec import migrate_data_spec
from .migrate_view_spec import migrate_view_spec
from ...metadata import write_project_metadata


def _migrate_project(root, ds_list, metadata, ds_file,
                     parse_source_name, parse_menu_name):
    for ds in ds_list:
        ds_folder = os.path.join(root, ds)
        assert os.path.exists(ds_folder), ds_folder
        print("Migrate dataset:", ds)
        file_formats = migrate_dataset(ds_folder, parse_menu_name=parse_menu_name,
                                       parse_source_name=parse_source_name)

    metadata['specVersion'] = '0.2.0'
    metadata["imageDataFormats"] = file_formats
    os.remove(ds_file)
    return metadata


def _update_view_spec(root, ds_list):
    for ds in ds_list:
        ds_folder = os.path.join(root, ds)
        assert os.path.exists(ds_folder), ds_folder
        migrate_view_spec(ds_folder)


def _update_data_spec(root, ds_list, metadata):
    for ds in ds_list:
        ds_folder = os.path.join(root, ds)
        assert os.path.exists(ds_folder), ds_folder
        file_formats = migrate_data_spec(ds_folder)
    metadata["imageDataFormats"] = file_formats
    return metadata


def migrate_project(root, parse_menu_name=None, parse_source_name=None, update_view_spec=False, update_data_spec=False):
    assert not (update_view_spec and update_data_spec)
    already_v2 = update_view_spec or update_data_spec

    ds_file = os.path.join(root, 'project.json') if already_v2 else os.path.join(root, 'datasets.json')
    with open(ds_file, 'r') as f:
        metadata = json.load(f)
    ds_list = metadata['datasets']

    if update_view_spec:
        _update_view_spec(root, ds_list)
    elif update_data_spec:
        metadata = _update_data_spec(root, ds_list, metadata)
    else:
        metadata = _migrate_project(root, ds_list, metadata, ds_file,
                                    parse_source_name, parse_menu_name)
    write_project_metadata(root, metadata)
