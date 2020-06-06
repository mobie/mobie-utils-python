import json
import os
import warnings


def _load_datasets(path):
    try:
        with open(path) as f:
            datasets = json.load(f)
    except (FileNotFoundError, ValueError):
        datasets = {}
        datasets['datasets'] = []
    return datasets


def have_dataset(root, dataset_name):
    path = os.path.join(root, 'datasets.json')
    datasets = _load_datasets(path)
    return dataset_name in datasets['datasets']


def add_dataset(root, dataset_name, is_default):
    path = os.path.join(root, 'datasets.json')
    datasets = _load_datasets(path)

    if dataset_name in datasets['datasets']:
        warnings.warn(f"Dataset {dataset_name} is already present!")
    else:
        datasets['datasets'].append(dataset_name)

    if is_default:
        datasets['defaultDataset'] = dataset_name

    with open(path, 'w') as f:
        json.dump(datasets, f, sort_keys=True, indent=2)
