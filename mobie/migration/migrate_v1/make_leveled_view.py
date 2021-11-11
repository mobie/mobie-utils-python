import json
import os
from glob import glob


def make_leveled_view(folder, normal_vector):
    assert len(normal_vector) == 3
    leveled_view = {"NormalVector": normal_vector}
    leveled_view_file = os.path.join(folder, 'misc', 'leveling.json')
    with open(leveled_view_file, 'w') as f:
        json.dump(leveled_view, f)


def make_leveled_views(root, pattern, normal_vector):
    assert len(normal_vector) == 3
    leveled_view = {"NormalVector": normal_vector}
    folders = glob(os.path.join(root, pattern))
    for folder in folders:
        leveled_view_file = os.path.join(folder, 'misc', 'leveling.json')
        with open(leveled_view_file, 'w') as f:
            json.dump(leveled_view, f)
