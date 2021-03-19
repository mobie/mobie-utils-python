import argparse
import json
import multiprocessing
import os

from cluster_tools.cluster_tasks import BaseClusterTask
import mobie.metadata as metadata


# TODO more name parsing
def get_default_menu_item(source_type, name):
    menu_item = f"{source_type}/name"
    return menu_item


# TODO default arguments for scale-factors and chunks
def get_base_parser(description, transformation_file=False):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--input_path', type=str,
                        help="path to the input data", required=True)
    parser.add_argument('--input_key', type=str,
                        help="key for the input data, e.g. internal path for h5/n5 data or patterns like *.tif",
                        required=True)
    parser.add_argument('--root', type=str,
                        help="root folder under which the MoBIE project is saved",
                        required=True)
    parser.add_argument('--dataset_name', type=str,
                        help="name of the dataset to which the image data is added",
                        required=True)
    parser.add_argument('--name', type=str,
                        help="name of the source to be added",
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

    parser.add_argument('--menu_item', type=str, default=None,
                        help="")
    parser.add_argument('--view', type=str, default=None,
                        help="default view settings for this source, json encoded or path to a json file")
    if transformation_file:
        parser.add_argument('--transformation', type=str, required=True,
                            help="file defining elastix transformation to be applied")
    else:
        parser.add_argument('--transformation', type=str, default=None,
                            help="affine transformation parameters in bdv convention, json encoded")
    parser.add_argument('--unit', type=str, default='micrometer',
                        help="physical unit of the source data")

    parser.add_argument('--tmp_folder', type=str, default=None,
                        help="folder for temporary computation files")
    parser.add_argument('--target', type=str, default='local',
                        help="computation target")
    parser.add_argument('--max_jobs', type=int, default=multiprocessing.cpu_count(),
                        help="number of jobs")
    return parser


def parse_spatial_args(args, parse_transformation=True):
    resolution = json.loads(args.resolution)
    if args.scale_factors is None:
        scale_factors = None
    else:
        scale_factors = json.loads(args.scale_factors)
    if args.chunks is None:
        chunks = None
    else:
        chunks = json.loads(args.chunks)

    if not parse_transformation:
        return resolution, scale_factors, chunks

    if args.transformation is None:
        transformation = None
    else:
        transformation = json.loads(args.transformation)
    return resolution, scale_factors, chunks, transformation


def parse_view(args):
    view = args.view
    if view is None:
        return view
    if os.path.exists(view):
        with open(view) as f:
            return json.loads(f)
    return json.loads(view)


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
    if not metadata.have_dataset(root, src_dataset):
        raise ValueError(f"Could not find dataset {src_dataset}")
    if metadata.have_dataset(root, dst_dataset):
        raise ValueError(f"A dataset with name {dst_dataset} is already present.")
    if copy_misc is not None and not callable(copy_misc):
        raise ValueError("copy_misc must be callable")

    dst_folder = metadata.create_dataset_structure(root, dst_dataset)
    src_folder = os.path.join(root, src_dataset)
    metadata.copy_dataset_folder(src_folder, dst_folder, copy_misc=copy_misc)

    metadata.add_dataset(root, dst_dataset, is_default)


def write_global_config(config_folder,
                        block_shape=None,
                        roi_begin=None,
                        roi_end=None,
                        qos=None):
    os.makedirs(config_folder, exist_ok=True)

    conf_path = os.path.join(config_folder, 'global.config')
    if os.path.exists(conf_path):
        with open(conf_path) as f:
            global_config = json.load(f)
    else:
        global_config = BaseClusterTask.default_global_config()

    if block_shape is not None:
        if len(block_shape) != 3:
            raise ValueError(f"Invalid block_shape given: {block_shape}")
        global_config['block_shape'] = block_shape

    if roi_begin is not None:
        if len(roi_begin) != 3:
            raise ValueError(f"Invalid roi_begin given: {roi_begin}")
        global_config['roi_begin'] = roi_begin

    if roi_end is not None:
        if len(roi_end) != 3:
            raise ValueError(f"Invalid roi_end given: {roi_end}")
        global_config['roi_end'] = roi_end

    if qos is not None:
        global_config['qos'] = qos

    with open(conf_path, 'w') as f:
        json.dump(global_config, f)
