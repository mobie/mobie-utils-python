import json
import os
from glob import glob

from utils import to_lower


def update_image_dict(image_dict):
    image_dict = to_lower(image_dict)
    new_image_dict = {}
    for name, entries in image_dict.items():
        updated_entries = {}
        if 'minValue' in entries:
            assert 'maxValue' in entries
            clims = [float(entries.pop('minValue')), float(entries.pop('maxValue'))]
            updated_entries['contrastLimits'] = clims
        if 'colorMap' in entries:
            updated_entries['color'] = entries.pop('colorMap')
        updated_entries.update(entries)
        new_image_dict[name] = updated_entries
    return new_image_dict


def migrate_image_dict(image_dict_path):
    with open(image_dict_path) as f:
        image_dict = json.load(f)
    image_dict = update_image_dict(image_dict)
    with open(image_dict_path, 'w') as f:
        json.dump(image_dict, f, indent=2, sort_keys=True)


def migrate_all_image_dicts(root, pattern):
    folders = glob(os.path.join(root, pattern))
    for folder in folders:
        image_dict = os.path.join(folder, 'images', 'images.json')
        migrate_image_dict(image_dict)
