import argparse
import json
import multiprocessing
import os

from mobie.metadata import add_to_image_dict, have_dataset
from mobie.import_data import import_segmentation


def add_mask(input_path, input_key,
             root, dataset_name, mask_name,
             resolution, scale_factors, chunks,
             tmp_folder=None, target='local',
             max_jobs=multiprocessing.cpu_count(),
             settings=None):
    """ Add a mask to an existing MoBIE dataset.

    Arguments:
        input_path [str] - path to the mask to add.
        input_key [str] - key to the mask to add.
        root [str] - data root folder.
        dataset_name [str] - name of the dataset the mask should be added to.
        mask_name [str] - name of the mask.
        resolution [list[float]] - resolution of the mask in micrometer.
        scale_factors [list[list[int]]] - scale factors used for down-sampling.
        chunks [list[int]] - chunks for the data.
        tmp_folder [str] - folder for temporary files (default: None)
        target [str] - computation target (default: 'local')
        max_jobs [int] - number of jobs (default: number of cores)
        settings [dict] - layer settings for the mask (default: None)
    """
    # check that we have this dataset
    if not have_dataset(root, dataset_name):
        raise ValueError(f"Dataset {dataset_name} not found in {root}")

    tmp_folder = f'tmp_{mask_name}' if tmp_folder is None else tmp_folder

    # import the mask data
    dataset_folder = os.path.join(root, dataset_name)
    data_path = os.path.join(dataset_folder, 'images', 'local', f'{mask_name}.n5')
    xml_path = os.path.join(dataset_folder, 'images', 'local', f'{mask_name}.xml')
    import_segmentation(input_path, input_key, data_path,
                        resolution, scale_factors, chunks,
                        tmp_folder=tmp_folder, target=target,
                        max_jobs=max_jobs, with_max_id=False)

    # add the mask to the image dict
    add_to_image_dict(dataset_folder, 'mask', xml_path)


def main():
    parser = argparse.ArgumentParser(description="Add image data to MoBIE dataset")
    parser.add_argument('input_path', type=str,
                        help="path to the input mask")
    parser.add_argument('input_key', type=str,
                        help="key for the input mask, e.g. internal path for h5/n5 data")
    parser.add_argument('root', type=str,
                        help="root folder of the MoBIE project")
    parser.add_argument('dataset_name', type=str,
                        help="name of the dataset to which the image data is added")
    parser.add_argument('mask_name', type=str,
                        help="name of the image data that is added")

    parser.add_argument('resolution', type=str,
                        help="resolution of the data in micrometer, json-encoded")
    parser.add_argument('scale_factors', type=str,
                        help="factors used for downscaling the data, json-encoded")
    parser.add_argument('chunks', type=str,
                        help="chunks of the data that is added, json-encoded")

    parser.add_argument('--tmp_folder', type=str, default=None,
                        help="folder for temporary computation files")
    parser.add_argument('--target', type=str, default='local',
                        help="computation target")
    parser.add_argument('--max_jobs', type=int, default=multiprocessing.cpu_count(),
                        help="number of jobs")

    args = parser.parse_args()

    # resolution, scale_factors and chunks need to be json encoded
    resolution = json.loads(args.resolution)
    scale_factors = json.loads(args.scale_factors)
    chunks = json.loads(args.chunks)

    add_mask(args.input_path, args.input_key,
             args.root, args.dataset_name, args.mask_name,
             resolution=resolution, scale_factors=scale_factors, chunks=chunks,
             tmp_folder=args.tmp_folder, target=args.target, max_jobs=args.max_jobs)
