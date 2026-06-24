"""General-purpose helper functions.
"""
import argparse
import json
import multiprocessing
import os
from copy import deepcopy
from typing import Callable, Dict, List, Optional, Tuple, Union

import h5py
import numpy as np
import elf.transformation as trafo_helper
import mobie.metadata as metadata

from bioimage_py.runner.config import RunnerConfig, SlurmConfig
from cluster_tools.cluster_tasks import BaseClusterTask
from elf.io import open_file
from mobie.validation import validate_view_metadata
from mobie.xml_utils import update_xml_transformation_parameter
from pybdv.util import get_key

FILE_FORMATS = [
    "bdv.hdf5",
    "bdv.n5",
    "bdv.n5.s3",
    "ome.zarr",
    "ome.zarr.s3",
    "openOrganelle.s3"
]
"""List of supported file formats.
"""


def get_data_key(file_format: str, scale: int, path: Optional[str] = None) -> str:
    """Get the key / internal path for data stored in one of the supported file formats.

    Args:
        file_format: The file format of the data.
        scale: The scale level to retrieve.
        path: The path to the data. Only required for data stored in ome.zarr format.

    Returns:
        The key / internal path to the data.
    """
    if file_format.startswith("bdv"):
        is_h5 = file_format == "bdv.hdf5"
        key = get_key(is_h5, timepoint=0, setup_id=0, scale=scale)
    elif file_format == "ome.zarr":
        assert path is not None
        with open_file(path, "r") as f:
            mscales = f.attrs["multiscales"][0]
            key = mscales["datasets"][0]["path"]
    else:
        raise NotImplementedError(file_format)
    return key


def get_internal_paths(dataset_folder: str, file_format: str, name: str) -> Tuple[str, str]:
    """Get the internal paths to source data stored in a MoBIE dataset.

    Args:
        dataset_folder: The folder of the MoBIE dataset.
        file_format: The file format in which the data is stored.
        name: The name of the source.

    Returns:
        The path to the data of the source.
        The path of the bdv xml file of the source. Only applicable if the file format is a bdv format.
    """
    if file_format not in FILE_FORMATS:
        raise ValueError(f"Unknown file format {file_format}.")

    file_format_ = file_format.replace(".", "-")
    if file_format == "bdv.hdf5":
        data_path = os.path.join(dataset_folder, "images", file_format_, f"{name}.h5")
        xml_path = os.path.join(dataset_folder, "images", file_format_, f"{name}.xml")
        return data_path, xml_path

    elif file_format == "bdv.n5":
        data_path = os.path.join(dataset_folder, "images", file_format_, f"{name}.n5")
        xml_path = os.path.join(dataset_folder, "images", file_format_, f"{name}.xml")
        return data_path, xml_path

    elif file_format == "ome.zarr":
        data_path = os.path.join(dataset_folder, "images", file_format_, f"{name}.ome.zarr")
        return data_path, data_path

    raise ValueError(f"Data creation for the file format {file_format} is not supported.")


def require_dataset(root: str, dataset_name: str) -> bool:
    """Check if a dataset in the given project exists, create the project if it does not exist.

    Args:
        root: The root directory of the MoBIE project.
        dataset_name: The name of the dataset.

    Returns:
        Whether the dataset exists in the project.
    """
    # Check if we have the project and dataset already.
    proj_exists = metadata.project_exists(root)
    if proj_exists:
        ds_exists = metadata.dataset_exists(root, dataset_name)
    else:
        metadata.create_project_metadata(root)
        ds_exists = False
    return ds_exists


def require_dataset_and_view(
    root: str,
    dataset_name: str,
    file_format: str,
    source_type: str,
    source_name: str,
    menu_name: str,
    view: Dict,
    is_default_dataset: bool,
    contrast_limits: Optional[List[float]] = None,
    description: Optional[str] = None,
) -> Dict:
    """Require that the specified dataset and given view exist.

    Args:
        root: The root directory of the MoBIE project.
        dataset_name: The name of the dataset.
        file_format: The file format in which the data is stored in the dataset.
        source_type: The type of the source ('image', 'segmentation', or 'spots').
        source_name: The name of the source.
        menu_name: The menu for the view.
        view: The view data.
        is_default_dataset: Whether to set new dataset as default dataset.
            Only applies if the dataset is being created.
        contrast_limits: The contrast limits. Only applicable for an image source.
        description: The description of this view.

    Returns:
        The view data.
    """
    ds_exists = require_dataset(root, dataset_name)

    dataset_folder = os.path.join(root, dataset_name)
    if view is None:
        kwargs = {"contrastLimits": contrast_limits} if source_type == "image" else {}
        view = metadata.get_default_view(
            source_type, source_name, menu_name=menu_name, description=description, **kwargs
        )
    elif view == {}:
        pass
    else:
        update_view = {}
        if menu_name is not None:
            update_view["uiSelectionGroup"] = menu_name
        if source_type == "image" and contrast_limits is not None:
            update_view["contrastLimits"] = contrast_limits
        if update_view:
            view.update(update_view)
    if view != {}:
        validate_view_metadata(view, sources=[source_name])

    if not ds_exists:
        assert file_format is not None
        metadata.create_dataset_structure(root, dataset_name, [file_format])
        default_view = deepcopy(view)
        default_view.update({"uiSelectionGroup": "bookmark"})
        metadata.create_dataset_metadata(dataset_folder, views={"default": default_view})
        metadata.add_dataset(root, dataset_name, is_default_dataset)

    return view


# TODO default arguments for scale-factors and chunks
def get_base_parser(description: str, transformation_file: bool = False):
    """Get the argument parser for CLI functionality for adding sources.

    Args:
        description: The description string for the CLI.
        transformation_file: Whether to add an argument for passing a transformation file.

    Returns:
        The argpument parser.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--input_path", type=str,
                        help="path to the input data", required=True)
    parser.add_argument("--input_key", type=str,
                        help="key for the input data, e.g. internal path for h5/n5 data or patterns like *.tif",
                        required=True)
    parser.add_argument("--root", type=str,
                        help="root folder under which the MoBIE project is saved",
                        required=True)
    parser.add_argument("--dataset_name", type=str,
                        help="name of the dataset to which the image data is added",
                        required=True)
    parser.add_argument("--name", type=str,
                        help="name of the source to be added",
                        required=True)

    parser.add_argument("--resolution", type=str,
                        help="resolution of the data in micrometer, json-encoded",
                        required=True)
    parser.add_argument("--scale_factors", type=str,
                        help="factors used for downscaling the data, json-encoded",
                        required=True)
    parser.add_argument("--chunks", type=str,
                        help="chunks of the data that is added, json-encoded",
                        required=True)

    parser.add_argument("--menu_name", type=str, default=None,
                        help="the menu name which will be used when grouping this source in the UI")
    parser.add_argument("--view", type=str, default=None,
                        help="default view settings for this source, json encoded or path to a json file")
    if transformation_file:
        parser.add_argument("--transformation", type=str, required=True,
                            help="file defining elastix transformation to be applied")
    else:
        parser.add_argument("--transformation", type=str, default=None,
                            help="affine transformation parameters in bdv convention, json encoded")
    parser.add_argument("--unit", type=str, default="micrometer",
                        help="physical unit of the source data")

    parser.add_argument("--tmp_folder", type=str, default=None,
                        help="folder for temporary computation files")
    parser.add_argument("--target", type=str, default="local",
                        help="computation target")
    parser.add_argument("--max_jobs", type=int, default=multiprocessing.cpu_count(),
                        help="number of jobs")

    hlp = "whether to set new dataset as default dataset. Only applies if the dataset is being created."
    parser.add_argument("--is_default_dataset", type=int, default=0, help=hlp)

    return parser


def parse_spatial_args(args, parse_transformation=True):
    """@private
    """
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
    """@private
    """
    view = args.view
    if view is None:
        return view
    if os.path.exists(view):
        with open(view) as f:
            return json.loads(f)
    return json.loads(view)


def clone_dataset(
    root: str,
    src_dataset: str,
    dst_dataset: str,
    is_default: bool = False,
    copy_misc: Optional[Callable] = None,
) -> None:
    """Initialize a MoBIE dataset by cloning an existing dataset.

    Args:
        root: The root folder of the MoBIE project.
        src_dataset: The name of the MoBIE dataset to be cloned.
        dst_dataset: The name of the MoBIE dataset to be added.
        is_default: Whether to set the new dataset as default dataset.
        copy_misc: An optional function to copy additonal misc data.
    """
    # check that we have the src dataset and don"t have the dst dataset already
    if not metadata.dataset_exists(root, src_dataset):
        raise ValueError(f"Could not find dataset {src_dataset}")
    if metadata.dataset_exists(root, dst_dataset):
        raise ValueError(f"A dataset with name {dst_dataset} is already present.")
    if copy_misc is not None and not callable(copy_misc):
        raise ValueError("copy_misc must be callable")

    file_formats = metadata.dataset_metadata.get_file_formats(os.path.join(root, src_dataset))
    dst_folder = metadata.create_dataset_structure(root, dst_dataset, file_formats)
    src_folder = os.path.join(root, src_dataset)
    metadata.copy_dataset_folder(src_folder, dst_folder, copy_misc=copy_misc)

    metadata.add_dataset(root, dst_dataset, is_default)


def get_run_config(
    target: str,
    max_jobs: int,
    tmp_folder: Optional[str] = None,
    qos: Optional[str] = None,
) -> Tuple[str, Optional[RunnerConfig], int]:
    """Map mobie's computation target onto a bioimage-py runner configuration.

    This is the foundation shim that replaces the cluster_tools luigi / global-config model:
    instead of writing `*.config` files, the computation target and job count are translated
    into bioimage-py's per-call `job_type` / `job_config` / `num_workers` arguments.

    Args:
        target: The computation target. One of 'local', 'subprocess' or 'slurm'.
        max_jobs: The number of jobs for parallelization (maps to bioimage-py `num_workers`).
        tmp_folder: The folder for temporary computation files. Used as the runner's shared
            `tmp_root` for the distributed targets ('subprocess' and 'slurm').
        qos: The slurm quality-of-service. Only applies to the 'slurm' target.

    Returns:
        The bioimage-py job type.
        The runner configuration (None for the 'local' target).
        The number of workers.

    Raises:
        ValueError: If the computation target is not supported. Note that 'lsf' is no longer
            supported.
    """
    if target == "local":
        return "local", None, max_jobs
    elif target == "subprocess":
        return "subprocess", RunnerConfig(tmp_root=tmp_folder), max_jobs
    elif target == "slurm":
        return "slurm", SlurmConfig(tmp_root=tmp_folder, qos=qos), max_jobs
    raise ValueError(
        f"Invalid computation target '{target}'. Supported targets are 'local', 'subprocess' and 'slurm'."
    )


def write_global_config(
    config_folder,
    block_shape=None,
    roi_begin=None,
    roi_end=None,
    qos=None,
    require3d=True
):
    """@private
    """
    os.makedirs(config_folder, exist_ok=True)

    conf_path = os.path.join(config_folder, "global.config")
    if os.path.exists(conf_path):
        with open(conf_path) as f:
            global_config = json.load(f)
    else:
        global_config = BaseClusterTask.default_global_config()

    if block_shape is not None:
        if require3d and len(block_shape) != 3:
            raise ValueError(f"Invalid block_shape given: {block_shape}")
        global_config["block_shape"] = block_shape

    if roi_begin is not None:
        # NOTE rois are only applicable if the data is 3d, so we don"t add the "require3d" check here
        if len(roi_begin) != 3:
            raise ValueError(f"Invalid roi_begin given: {roi_begin}")
        global_config["roi_begin"] = roi_begin

    if roi_end is not None:
        # NOTE rois are only applicable if the data is 3d, so we don"t add the "require3d" check here
        if len(roi_end) != 3:
            raise ValueError(f"Invalid roi_end given: {roi_end}")
        global_config["roi_end"] = roi_end

    if qos is not None:
        global_config["qos"] = qos

    with open(conf_path, "w") as f:
        json.dump(global_config, f)


def transformation_to_xyz(transform: Union[List, np.ndarray], invert: bool = False) -> List[float]:
    """Convert a transformation from zyx coordinates (python default) to xyz coordinates (expected by mobie).

    Args:
        transform: The transformation parameters. It must contain 12 values,
            corresponding to the upper 3 rows of the affine matrix.
        invert: Whether to invert the transformation.
            This may be necessary because e.g. scipy uses the different transformation direction.

    Returns:
        The transformation parameters in the format expected by MoBIE.
    """
    trafo = trafo_helper.parameters_to_matrix(transform)
    trafo = trafo_helper.native_to_bdv(trafo, invert=invert)
    return trafo


def save_temp_input(data, tmp_folder, name):
    """@private
    """
    os.makedirs(tmp_folder, exist_ok=True)

    save_path = os.path.join(tmp_folder, f"{name}.h5")
    save_key = "data"

    with h5py.File(save_path, "a") as f:
        if save_key in f:
            return save_path, save_key
        f.create_dataset(save_key, data=data, compression="gzip")

    return save_path, save_key


# TODO implement this once ome.zarr v0.5 is released
def update_ome_zarr_transformation_parameter(metadata_path, parameter):
    """@private
    """
    raise NotImplementedError(
        "Transformations in the image metadata are currently not supported for the ome.zarr file format."
        "You can use the bdv.n5 format instead."
    )


def update_transformation_parameter(metadata_path, parameter, file_format):
    """@private
    """
    if file_format.startswith("bdv"):
        assert os.path.splitext(metadata_path)[1] == ".xml"
        update_xml_transformation_parameter(metadata_path, parameter)
    elif file_format.startswith("ome.zarr"):
        update_ome_zarr_transformation_parameter(metadata_path, parameter)
    else:
        raise NotImplementedError(
            f"Setting parameters in the image metadata is not supported for the {file_format} format"
        )
