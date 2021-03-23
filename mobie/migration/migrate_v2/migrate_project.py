import json
import os
from .migrate_dataset import migrate_dataset


def migrate_project(root):
    ds_file = os.path.join(root, 'datasets.json')
    with open(ds_file, 'r') as f:
        datasets = json.load(f)
    ds_list = datasets['datasets']
    for ds in ds_list:
        ds_folder = os.path.join(root, ds)
        assert os.path.exists(ds_folder), ds_folder
        print("Migrate dataset", ds)
        migrate_dataset(ds_folder)

    datasets['specVersion'] = '0.2.0'
    with open(ds_file, 'w') as f:
        json.dump(datasets, f, indent=2, sort_keys=True)
