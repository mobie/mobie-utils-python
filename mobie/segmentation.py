import multiprocessing
import os

from pybdv.metadata import get_key
from .import_data import import_segmentation
from .metadata import add_to_image_dict, have_dataset
from .tables import compute_default_table


def add_segmentation(input_path, input_key,
                     root, dataset_name, segmentation_name,
                     resolution, scale_factors, chunks,
                     tmp_folder=None, target='local',
                     max_jobs=multiprocessing.cpu_count(),
                     add_default_table=True, settings=None):
    """ Add a segmentation to an existing MoBIE dataset.

    Arguments:
        input_path [str] - path to the segmentation to add.
        input_key [str] - key to the segmentation to add.
        root [str] - data root folder.
        dataset_name [str] - name of the dataset the segmentation should be added to.
        segmentation_name [str] - name of the segmentation.
        resolution [list[float]] - resolution of the segmentation in micrometer.
        scale_factors [list[list[int]]] - scale factors used for down-sampling.
        chunks [list[int]] - chunks for the data.
        tmp_folder [str] - folder for temporary files (default: None)
        target [str] - computation target (default: 'local')
        max_jobs [int] - number of jobs (default: number of cores)
        add_default_table [bool] - whether to add the default table (default: True)
        settings [dict] - layer settings for the segmentation (default: None)
    """
    # check that we have this dataset
    if not have_dataset(root, dataset_name):
        raise ValueError(f"Dataset {dataset_name} not found in {root}")

    tmp_folder = f'tmp_{segmentation_name}' if tmp_folder is None else tmp_folder

    # import the segmentation data
    dataset_folder = os.path.join(root, dataset_name)
    data_path = os.path.join(dataset_folder, 'images', 'local', f'{segmentation_name}.n5')
    xml_path = os.path.join(dataset_folder, 'images', 'local', f'{segmentation_name}.xml')
    import_segmentation(input_path, input_key, data_path,
                        resolution, scale_factors, chunks,
                        tmp_folder=tmp_folder, target=target,
                        max_jobs=max_jobs)

    # compute the default segmentation table
    if add_default_table:
        table_folder = os.path.join(dataset_folder, 'tables', segmentation_name)
        table_path = os.path.join(table_folder, 'default.csv')
        os.makedirs(table_folder, exist_ok=True)
        key = get_key(False, 0, 0, 0)
        compute_default_table(data_path, key, table_path, resolution,
                              tmp_folder=tmp_folder, target=target,
                              max_jobs=max_jobs)
    else:
        table_folder = None

    # add the segmentation to the image dict
    add_to_image_dict(dataset_folder, 'segmentation', xml_path,
                      table_folder=table_folder)
