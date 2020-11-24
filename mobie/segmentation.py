import argparse
import json
import multiprocessing
import os

from pybdv.metadata import get_key
from mobie.import_data import (import_segmentation,
                               import_segmentation_from_node_labels,
                               import_segmentation_from_paintera,
                               is_paintera)
from mobie.metadata import add_to_image_dict, have_dataset
from mobie.tables import compute_default_table


def add_segmentation(input_path, input_key,
                     root, dataset_name, segmentation_name,
                     resolution, scale_factors, chunks,
                     node_label_path=None, node_label_key=None,
                     tmp_folder=None, target='local',
                     max_jobs=multiprocessing.cpu_count(),
                     add_default_table=True, settings=None,
                     postprocess_config=None):
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
        node_label_path [str] - path to node labels (default: None)
        node_label_key [str] - key to node labels (default: None)
        tmp_folder [str] - folder for temporary files (default: None)
        target [str] - computation target (default: 'local')
        max_jobs [int] - number of jobs (default: number of cores)
        add_default_table [bool] - whether to add the default table (default: True)
        settings [dict] - layer settings for the segmentation (default: None)
        postprocess_config [dict] - config for postprocessing,
            only available for paintera dataset (default: None)
    """
    # check that we have this dataset
    if not have_dataset(root, dataset_name):
        raise ValueError(f"Dataset {dataset_name} not found in {root}")

    tmp_folder = f'tmp_{segmentation_name}' if tmp_folder is None else tmp_folder

    # import the segmentation data
    dataset_folder = os.path.join(root, dataset_name)
    data_path = os.path.join(dataset_folder, 'images', 'local', f'{segmentation_name}.n5')
    xml_path = os.path.join(dataset_folder, 'images', 'local', f'{segmentation_name}.xml')
    if node_label_path is not None:
        if node_label_key is None:
            raise ValueError("Expect node_label_key if node_label_path is given")
        import_segmentation_from_node_labels(input_path, input_key, data_path,
                                             node_label_path, node_label_key,
                                             resolution, scale_factors, chunks,
                                             tmp_folder=tmp_folder, target=target,
                                             max_jobs=max_jobs)
    elif is_paintera(input_path, input_key):
        import_segmentation_from_paintera(input_path, input_key, data_path,
                                          resolution, scale_factors, chunks,
                                          tmp_folder=tmp_folder, target=target,
                                          max_jobs=max_jobs,
                                          postprocess_config=postprocess_config)
    else:
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


def main():
    parser = argparse.ArgumentParser(description="Add segmentation to MoBIE dataset")
    parser.add_argument('--input_path', type=str,
                        help="path to the input segmentation",
                        required=True)
    parser.add_argument('--input_key', type=str,
                        help="key for the input segmentation, e.g. internal path for h5/n5 data",
                        required=True)
    parser.add_argument('--root', type=str,
                        help="root folder of the MoBIE project",
                        required=True)
    parser.add_argument('--dataset_name', type=str,
                        help="name of the dataset to which the segmentation is added",
                        required=True)
    parser.add_argument('--segmentation_name', type=str,
                        help="name of the segmentation that is added",
                        required=True)

    parser.add_argument('--resolution', type=str,
                        help="resolution of the data in micrometer, json-encoded",
                        required=True)
    parser.add_argument('--scale_factors', type=str,
                        help="factors used for downscaling the data, json-encoded",
                        required=True)
    parser.add_argument('--chunks', type=str,
                        help="chunks of the data that is added, json-encoded",
                        required=True)

    parser.add_argument('--node_label_path', type=str, default=None,
                        help="path to the node_labels for the segmentation")
    parser.add_argument('--node_label_key', type=str, default=None,
                        help="key for the node labels for segmentation")

    parser.add_argument('--add_default_table', type=int, default=1,
                        help="whether to add the default table")
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

    add_segmentation(args.input_path, args.input_key,
                     args.root, args.dataset_name, args.segmentation_name,
                     node_label_path=args.node_label_path, node_label_key=args.node_label_key,
                     resolution=resolution, add_default_table=bool(args.add_default_table),
                     scale_factors=scale_factors, chunks=chunks,
                     tmp_folder=args.tmp_folder, target=args.target, max_jobs=args.max_jobs)
