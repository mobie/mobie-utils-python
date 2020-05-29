import os
import json


def versions_to_datasets(root):
    with open(os.path.join(root, 'versions.json')) as f:
        versions = json.load(f)

    datasets = {
        "datasets": versions,
        "defaultDataset": versions[-1]
    }

    with open(os.path.join(root, 'datasets.json'), 'w') as f:
        json.dump(datasets, f, indent=2, sort_keys=True)
