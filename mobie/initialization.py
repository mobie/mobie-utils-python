import json
import multiprocessing
import os
import warnings

from .layer_settings import (default_image_layer_settings,
                             validate_layer_settings)
from .import_data import import_raw_volume


def initialize_dataset(input_path, input_key,
                       root, dataset_name, raw_name,
                       resolution, chunks, scale_factors,
                       is_default=False, tmp_folder=None, target='local',
                       max_jobs=multiprocessing.cpu_count(), time_limit=None):
    """ Initialize a MoBIE dataset by copying raw data and creating the dataset folder.

    Arguments:
        input_path [str] -
    """
    dataset_folder = make_dataset_folders(root, dataset_name)

    if tmp_folder is None:
        tmp_folder = f'tmp_{dataset_name}'

    data_path = os.path.join(dataset_folder, 'images', 'local', f'{raw_name}.n5')
    xml_path = os.path.join(dataset_folder, 'images', 'local', f'{raw_name}.xml')

    import_raw_volume(input_path, input_key, data_path,
                      resolution, scale_factors, chunks,
                      tmp_folder=tmp_folder, target=target, max_jobs=max_jobs)

    initialize_image_dict(dataset_folder, xml_path)

    initialize_bookmarks(dataset_folder, raw_name)

    add_dataset(root, dataset_name, is_default)


def make_dataset_folders(root, dataset_name):
    """ Make the folder structure for a new dataset.

    Arguments:
        root [str] - the root data directory
        dataset_name [str] - name of the dataset
    """
    dataset_folder = os.path.join(root, dataset_name)
    os.makedirs(os.path.join(dataset_folder, 'tables'), exist_ok=True)
    os.makedirs(os.path.join(dataset_folder, 'images', 'local'), exist_ok=True)
    os.makedirs(os.path.join(dataset_folder, 'images', 'remote'), exist_ok=True)
    os.makedirs(os.path.join(dataset_folder, 'misc'), exist_ok=True)
    return dataset_folder


def add_dataset(root, dataset_name, is_default):
    path = os.path.join(root, 'datasets.json')
    try:
        with open(path) as f:
            datasets = json.load(f)
    except (FileNotFoundError, ValueError):
        datasets = {}
        datasets['datasets'] = []

    if dataset_name in datasets['datasets']:
        warnings.warn(f"Dataset {dataset_name} is already present!")
    else:
        datasets['datasets'].append(dataset_name)

    if is_default:
        datasets['defaultDataset'] = dataset_name

    with open(path, 'w') as f:
        json.dump(datasets, f, sort_keys=True, indent=2)


def initialize_image_dict(folder, xml_path, layer_settings=None):
    """ Initialize the image dict for a new dataset.

    Arguments:
        folder [str] - path to the dataset folder.
        xml_path [str] - path to the xml for the raw data of this dataset.
        layer_settings [dict] - settings for the raw layer. (default: None)
    """

    assert os.path.exists(xml_path), xml_path

    image_folder = os.path.join(folder, 'images')
    image_dict_path = os.path.join(image_folder, 'images.json')

    raw_name = os.path.splitext(os.path.split(xml_path)[1])[0]
    rel_path = os.path.relpath(xml_path, image_folder)

    # TODO
    if layer_settings is None:
        layer_settings = default_image_layer_settings()
    else:
        validate_layer_settings(layer_settings)

    layer_settings.update({
        "storage": {
            "local": rel_path,
            "remote": rel_path.replace("local", "remote")
        }
    })

    image_dict = {
        raw_name: layer_settings
    }

    with open(image_dict_path, 'w') as f:
        json.dump(image_dict, f, indent=2, sort_keys=True)


def initialize_bookmarks(folder, raw_name, layer_settings=None):
    """ Initialize the boomkmarks for a new dataset.

    Arguments:
        folder [str] - path to the dataset folder.
        raw_name [str] - name of the raw data for this dataset.
        layer_settings [dict] - settings for the layer. (default: None)
    """

    bookmark_folder = os.path.join(folder, 'misc', 'bookmarks')
    os.makedirs(bookmark_folder, exist_ok=True)
    bookmark_path = os.path.join(bookmark_folder, 'default.json')

    if layer_settings is None:
        layer_settings = {
            'contrastLimits': default_image_layer_settings()['contrastLimits']
        }
    else:
        validate_layer_settings(layer_settings)

    bkmrk = {
        'default': {
            'layers': {
                raw_name: layer_settings
            }
        }
    }

    with open(bookmark_path, 'w') as f:
        json.dump(bkmrk, f, indent=2, sort_keys=True)
