import argparse
import multiprocessing
import os

from elf.io import open_file
from mobie.import_data import apply_registration
from mobie.import_data.util import downscale
from mobie.metadata import add_to_image_dict, have_dataset
from mobie.tables import compute_default_table


def copy_label_id(in_path, in_key, out_path, out_key):
    with open_file(in_path, 'r') as f:
        ds = f[in_key]
        max_id = ds.attrs.get('maxId', None)
    if max_id is not None:
        with open_file(out_path, 'a') as f:
            ds = f[out_key]
            ds.attrs['maxId'] = max_id


def add_registered_volume(input_path, input_key, transformation,
                          root, dataset_name, data_name,
                          resolution, scale_factors, chunks,
                          method='affine', image_type='image', add_default_table=True,
                          fiji_executable=None, elastix_directory=None,
                          tmp_folder=None, target='local',
                          max_jobs=multiprocessing.cpu_count()):
    """ Add a volume after registration in elastix format.

    Arguments:
        input_path [str] - path to the data that should be added.
        input_key [str] - key to the data that should be added.
        transformation [str] - file defining elastix transformation to be applied
        root [str] - data root folder.
        dataset_name [str] - name of the dataset the data should be added to.
        data_name [str] - name of the data.
        resolution [list[float]] - resolution of the data in micrometer.
        scale_factors [list[list[int]]] - scale factors used for down-sampling.
        chunks [list[int]] - chunks for the data.
        method [str] - method used to apply the registration transformation:
            'affine': apply transformation using elf/nifty functionality.
                only works for affine transformations or simpler.
            'bdv': write transformation to bdv metadata so that it's applied on the fly.
                only works for affine transformations or simpler.
            'transformix': apply transformation using transformix
            (default: 'affine')
        image_type [str] - type of the data, can be either 'image', 'segmentation' or 'mask'
            (default: 'image')
        add_default_table [bool] - whether to add the default table (default: True)
        tmp_folder [str] - folder for temporary files (default: None)
        target [str] - computation target (default: 'local')
        max_jobs [int] - number of jobs (default: number of cores)
    """
    # check that we have this dataset
    if not have_dataset(root, dataset_name):
        raise ValueError(f"Dataset {dataset_name} not found in {root}")
    tmp_folder = f'tmp_{data_name}' if tmp_folder is None else tmp_folder

    dataset_folder = os.path.join(root, dataset_name)
    data_path = os.path.join(dataset_folder, 'images', 'local', f'{data_name}.n5')
    xml_path = os.path.join(dataset_folder, 'images', 'local', f'{data_name}.xml')
    data_key = 'setup0/timepoint0/s0'

    interpolation = 'linear' if image_type == 'image' else 'nearest'
    apply_registration(input_path, input_key, data_path, data_key,
                       transformation, method, interpolation,
                       fiji_executable=fiji_executable, elastix_directory=elastix_directory,
                       resolution=resolution, chunks=chunks,
                       tmp_folder=tmp_folder, target=target, max_jobs=max_jobs)

    data_key = 'setup0/timepoint0/s0'
    # we only need to downscale for a method that actually copies the data
    # (i.e. not for bdv, which just writes a new xml with a different affine trafo)
    if method != 'bdv':
        if image_type == 'image':
            ds_library = 'skimage'
            ds_library_kwargs = {}
        else:
            ds_library = 'vigra'
            ds_library_kwargs = {'order': 0}
        downscale(data_path, data_key, data_path,
                  resolution, scale_factors, chunks,
                  tmp_folder, target, max_jobs, block_shape=chunks,
                  library=ds_library, library_kwargs=ds_library_kwargs)
        copy_label_id(input_path, input_key, data_path, data_key)

    # compute the default segmentation table
    if image_type == 'segmentation' and add_default_table:
        if method == 'bdv':  # TODO implement this via on the fly transformation
            raise NotImplementedError
        table_folder = os.path.join(dataset_folder, 'tables', data_name)
        table_path = os.path.join(table_folder, 'default.csv')
        os.makedirs(table_folder, exist_ok=True)
        compute_default_table(data_path, data_key, table_path, resolution,
                              tmp_folder=tmp_folder, target=target,
                              max_jobs=max_jobs)
    else:
        table_folder = None

    # add the segmentation to the image dict
    add_to_image_dict(dataset_folder, image_type, xml_path,
                      table_folder=table_folder)


if __name__ == '__main__':
    # TODO finish argument-parser
    parser = argparse.ArgumentParser()
    parser.add_argument('input_path', type=str)
    parser.add_argument('input_key', type=str)
    args = parser.parse_args()
    add_registered_volume(args.input_path, args.input_key)
