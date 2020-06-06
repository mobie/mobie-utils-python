import json
import multiprocessing
import os
import warnings

from .import_data import import_raw_volume
from .metadata import add_to_image_dict, add_bookmark


def clone_dataset(root, input_dataset_name, dataset_name):
    pass


def initialize_dataset(input_path, input_key,
                       root, dataset_name, raw_name,
                       resolution, chunks, scale_factors,
                       is_default=False, tmp_folder=None, target='local',
                       max_jobs=multiprocessing.cpu_count(), time_limit=None):
    """ Initialize a MoBIE dataset by copying raw data and creating the dataset folder.

    Arguments:
        input_path [str] - path to the input raw data.
            Can be hdf5, n5/zarr, tif slices, knossos file or mrc file.
        input_key [str] - path in dataset for hdf5/n5/zatt, file pattern for tif
        root [str] - root folder for the MoBIE datasets
        dataset_name [str] - name of the MoBIE dataset to be added
        raw_name [str] - name of the raw data
        resolution [tuple[float] or list[float]] - resolution of the data in microns
        chunks [tuple[int] or list[int]] - chunks of the data to be written
        scale_factors [list[list[int]]] - downscaling factors for the data to be written
        is_default [bool] - set this dataset as default dataset (default: False)
        tmp_folder [str] - folder for temporary files (default: None)
        target [str] - computation target (default: 'local')
        max_jobs [int] - number of jobs (default: number of cores)
        time_limit [int] - time limit for job on cluster (default: None)
    """
    dataset_folder = make_dataset_folders(root, dataset_name)

    if tmp_folder is None:
        tmp_folder = f'tmp_{dataset_name}'

    data_path = os.path.join(dataset_folder, 'images', 'local', f'{raw_name}.n5')
    xml_path = os.path.join(dataset_folder, 'images', 'local', f'{raw_name}.xml')

    import_raw_volume(input_path, input_key, data_path,
                      resolution, scale_factors, chunks,
                      tmp_folder=tmp_folder, target=target, max_jobs=max_jobs)

    add_to_image_dict(dataset_folder, 'image', xml_path)
    add_bookmark(dataset_folder, 'default', 'default', raw_name,
                 settings={'contrastLimits': [0., 255.]})

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
