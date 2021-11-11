import json
import os
import subprocess
from glob import glob

from .utils import to_lower


def update_layers(layers):
    new_layers = {}
    for name, entries in layers.items():
        updated_entries = {}
        if 'minValue' in entries:
            assert 'maxValue' in entries
            clims = [float(entries.pop('minValue')), float(entries.pop('maxValue'))]
            updated_entries['contrastLimits'] = clims
        if 'colorMapMinValue' in entries:
            assert 'colorMapMaxValue' in entries
            clims = [float(entries.pop('colorMapMinValue')), float(entries.pop('colorMapMaxValue'))]
            updated_entries['contrastLimits'] = clims
        if 'colorMap' in entries:
            updated_entries['color'] = entries.pop('colorMap')
        updated_entries.update(entries)
        new_layers[name] = updated_entries
    return new_layers


def update_bookmarks(bookmarks):
    new_bookmarks = {}
    for name, bkmrk in bookmarks.items():
        bkmrk = to_lower(bkmrk)
        if 'layers' in bkmrk:
            layers = update_layers(bkmrk.pop('layers'))
            new_bkmrk = {'layers': layers}
        else:
            new_bkmrk = {}
        new_bkmrk.update(bkmrk)

        new_bookmarks[name] = new_bkmrk

    return new_bookmarks


def migrate_bookmarks(misc_folder, old_bookmark_name='manuscript_bookmarks'):
    bookmark_folder = os.path.join(misc_folder, 'bookmarks')
    os.makedirs(bookmark_folder, exist_ok=True)

    # make the default bookmark
    default_bookmark_path = os.path.join(bookmark_folder, 'default.json')
    default_bookmark = {
        "default": {
            "layers": {
                "sbem-6dpf-1-whole-raw": {
                    "contrastLimits": [0., 255.]
                }
            }
        }
    }
    with open(default_bookmark_path, 'w') as f:
        json.dump(default_bookmark, f, indent=2, sort_keys=True)

    # copy the paper bookmarks
    old_bookmark_path = os.path.join(misc_folder, 'bookmarks.json')
    if not os.path.exists(old_bookmark_path):
        return

    with open(old_bookmark_path) as f:
        bookmarks = json.load(f)
    if len(bookmarks) == 0:
        return

    bookmarks = update_bookmarks(bookmarks)

    new_bookmark_path = os.path.join(bookmark_folder, f'{old_bookmark_name}.json')
    with open(new_bookmark_path, 'w') as f:
        json.dump(bookmarks, f, indent=2, sort_keys=True)

    subprocess.run(['git', 'rm', old_bookmark_path])


def migrate_all_bookmarks(root, pattern):
    folders = glob(os.path.join(root, pattern))
    for folder in folders:
        misc_folder = os.path.join(folder, 'misc')
        migrate_bookmarks(misc_folder)
