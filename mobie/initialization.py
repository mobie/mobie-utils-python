import multiprocessing
import os

from .import_data import import_raw_volume
from .metadata import (add_bookmark, add_dataset, add_to_image_dict,
                       copy_dataset_folder, have_dataset)


def clone_dataset(root, src_dataset, dst_dataset, is_default=False, copy_misc=None):
    """ Initialize a MoBIE dataset by cloning an existing dataset.

    Arguments:
        root [str] - root folder of the MoBIE project
        src_dataset [str] - name of the MoBIE dataset to be cloned
        dst_dataset [str] - name of the MoBIE dataset to be added
        is_default [bool] - set this dataset as default dataset (default: False)
        copy_misc [callable] - function to copy additonal misc data (default: None)
    """
    # check that we have the src dataset and don't have the dst dataset already
    if not have_dataset(root, src_dataset):
        raise ValueError(f"Could not find dataset {src_dataset}")
    if have_dataset(root, dst_dataset):
        raise ValueError(f"A dataset with name {dst_dataset} is already present.")
    if copy_misc is not None and not callable(copy_misc):
        raise ValueError("copy_misc must be callable")

    dst_folder = make_dataset_folders(root, dst_dataset)
    src_folder = os.path.join(root, src_dataset)
    copy_dataset_folder(src_folder, dst_folder, copy_misc=copy_misc)

    add_dataset(root, dst_dataset, is_default)


def initialize_dataset(input_path, input_key,
                       root, dataset_name, raw_name,
                       resolution, chunks, scale_factors,
                       is_default=False,
                       tmp_folder=None, target='local',
                       max_jobs=multiprocessing.cpu_count(), time_limit=None):
    """ Initialize a MoBIE dataset by copying raw data and creating the dataset folder.

    Arguments:
        input_path [str] - path to the input raw data.
            Can be hdf5, n5/zarr, tif slices, knossos file or mrc file.
        input_key [str] - path in dataset for hdf5/n5/zatt, file pattern for tif
        root [str] - root folder of the MoBIE project
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
    if have_dataset(root, dataset_name):
        raise ValueError(f"A dataset with name {dataset_name} is already present.")

    dataset_folder = make_dataset_folders(root, dataset_name)

    if tmp_folder is None:
        tmp_folder = f'tmp_{dataset_name}'

    data_path = os.path.join(dataset_folder, 'images', 'local', f'{raw_name}.n5')
    xml_path = os.path.join(dataset_folder, 'images', 'local', f'{raw_name}.xml')

    import_raw_volume(input_path, input_key, data_path,
                      resolution, scale_factors, chunks,
                      tmp_folder=tmp_folder, target=target, max_jobs=max_jobs)

    add_to_image_dict(dataset_folder, 'image', xml_path)
    add_bookmark(dataset_folder, 'default', 'default',
                 layer_settings={raw_name: {'contrastLimits': [0., 255.]}})

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
    os.makedirs(os.path.join(dataset_folder, 'misc', 'bookmarks'), exist_ok=True)
    return dataset_folder
