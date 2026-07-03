"""Add a source with an elastix registration transformation.
"""
import multiprocessing
import os
import warnings
from typing import Dict, List, Optional, Sequence

import mobie.metadata as metadata
import mobie.utils as utils
from mobie.import_data.utils import downscale, add_max_id, get_scale_key
from mobie.tables import compute_default_table

try:
    from mobie.import_data.registration import apply_registration
except ImportError as e:
    warnings.warn(f"Could not import 'apply_registration' due to {e}.\n 'add_registered_volume' will not be available.")
    apply_registration = None


def add_registered_source(
    input_path: str,
    input_key: str,
    transformation: str,
    root: str,
    dataset_name: str,
    source_name: str,
    resolution: Sequence[float],
    scale_factors: Sequence[Sequence[int]],
    chunks: Sequence[int],
    method: str,
    menu_name: Optional[str] = None,
    file_format: str = "ome.zarr",
    shape: Optional[Sequence[int]] = None,
    source_type: str = "image",
    view: Optional[Dict] = None,
    add_default_table: bool = True,
    fiji_executable: Optional[str] = None,
    elastix_directory: Optional[str] = None,
    tmp_folder: Optional[str] = None,
    target: str = "local",
    max_jobs: int = multiprocessing.cpu_count(),
    bounding_box: Optional[List[List[int]]] = None,
    is_default_dataset: bool = False,
    description: Optional[str] = None,
    shards: Optional[Sequence[int]] = None,
) -> None:
    """Add a volume after registration in elastix format.

    Args:
        input_path: The path to the data that should be added.
        input_key: The key to the data that should be added.
        transformation: The file defining elastix transformation to be applied.
        root: The data root folder.
        dataset_name: The name of the dataset the data should be added to.
        source_name: The name of the data.
        resolution: The resolution of the data in micrometer.
        scale_factors: The scale factors used for down-sampling.
        chunks: The chunks for the data.
        method: The method used to apply the registration transformation:
            'affine': apply transformation using elf/nifty functionality.
                only works for affine transformations or simpler.
            'coordinate': apply transformation based on coordinate transformation of transformix
            'bdv': write transformation to bdv metadata so that it's applied on the fly.
                only works for affine transformations or simpler.
            'transformix': apply transformation using transformix
        menu_name: The menu name for this source.
            If none is given will be created based on the image name.
        file_format: The file format used to store the data internally.
        shape: The shape of the output volume.
            If None, the shape specified in the elastix transformation file will be used.
        source_type: The type of the data, can be either 'image', 'segmentation' or 'mask'.
        add_default_table: Whether to add the default table.
        tmp_folder: The folder fo temporary files.
        target: The computation target.
        max_jobs: The number of jobs.
        bounding_box: Theounding box where the registration is applied.
            needs to be specified in the output dataset space.
        is_default_dataset: Whether to set new dataset as default dataset. Only applies if the dataset is created.
        description: The description of this source.
        shards: The shard shape for zarr v3 sharding. Only supported for the ome.zarr v0.5 format
            (pass file_format='ome.zarr@0.5').
    """
    if apply_registration is None:
        raise ValueError("Could not import 'apply_registration' functionality")

    # the ome.zarr / NGFF version may be encoded as a suffix on the file format (e.g. 'ome.zarr@0.5').
    file_format, ome_zarr_version = utils.parse_file_format(file_format)
    utils.check_shards(shards, file_format, ome_zarr_version)

    view = utils.require_dataset_and_view(root, dataset_name, file_format,
                                          source_type=source_type, source_name=source_name,
                                          menu_name=menu_name, view=view,
                                          is_default_dataset=is_default_dataset)

    dataset_folder = os.path.join(root, dataset_name)
    tmp_folder = f"tmp_{source_name}" if tmp_folder is None else tmp_folder

    data_key = get_scale_key(file_format, 0)
    data_path, image_metadata_path = utils.get_internal_paths(dataset_folder, file_format, source_name)

    interpolation = "linear" if source_type == "image" else "nearest"
    # the resolution might be changed after the registration, which we need to take into account
    # in the subsequent downscaling step
    effective_resolution = apply_registration(input_path, input_key, data_path, data_key,
                                              transformation, method, interpolation,
                                              fiji_executable=fiji_executable, elastix_directory=elastix_directory,
                                              shape=shape, resolution=resolution, chunks=chunks,
                                              tmp_folder=tmp_folder, target=target, max_jobs=max_jobs,
                                              bounding_box=bounding_box, file_format=file_format,
                                              ome_zarr_version=ome_zarr_version, shards=shards)

    data_key = get_scale_key(file_format, 0)
    # we don"t need to downscale the data if the transformation is applied on the fly by bdv
    if method != "bdv":
        if source_type == "image":
            ds_library = "skimage"
            ds_library_kwargs = {}
        else:
            ds_library = "vigra"
            ds_library_kwargs = {"order": 0}
        downscale(data_path, data_key, data_path,
                  effective_resolution, scale_factors, chunks,
                  tmp_folder, target, max_jobs, block_shape=chunks,
                  library=ds_library, library_kwargs=ds_library_kwargs,
                  metadata_format=file_format, source_name=source_name,
                  ome_zarr_version=ome_zarr_version, shards=shards)
        add_max_id(input_path, input_key, data_path, data_key,
                   tmp_folder, target, max_jobs)

    # compute the default segmentation table
    if source_type == "segmentation" and add_default_table:

        if method == "bdv":
            msg = ("Cannot compute table for segmentation registered via bdv on-the-fly transformations."
                   "Use methd 'affine' instead to support this feature.")
            raise NotImplementedError(msg)

        table_folder = os.path.join(dataset_folder, "tables", source_name)
        table_path = os.path.join(table_folder, "default.tsv")
        os.makedirs(table_folder, exist_ok=True)
        compute_default_table(data_path, data_key, table_path, effective_resolution,
                              tmp_folder=tmp_folder, target=target,
                              max_jobs=max_jobs)
    else:
        table_folder = None

    # add the segmentation to the image dict
    metadata.add_source_to_dataset(dataset_folder, source_type,
                                   source_name, image_metadata_path,
                                   view=view, table_folder=table_folder,
                                   description=description)


def main():
    """@private
    """
    description = "Apply transformation defined in elastix format to source and add it to MoBIE dataset."
    parser = utils.get_base_parser(description, transformation_file=True)

    descr = """method used to apply the registration transformation:
            'affine': apply transformation using elf/nifty functionality.
                only works for affine transformations or simpler.
            'bdv': write transformation to bdv metadata so that it's applied on the fly.
                only works for affine transformations or simpler.
            'transformix': apply transformation using transformix
            'coordinate': apply transformation based on coordinate transformation of transformix
            """
    parser.add_argument('--method', type=str, required=True, help=descr)
    descr = """shape of the output volume. If None, the shape specified in
            the elastix transformation file will be used.
            """
    parser.add_argument('--shape', type=int, default=None, nargs=3,
                        help="")
    parser.add_argument('--source_type', type=str, default='image',
                        help="Type of the source, either 'image' or 'segmentation'")
    parser.add_argument('--add_default_table', type=int, default=1,
                        help="Add the default table for segmentations.")

    args = parser.parse_args()

    resolution, scale_factors, chunks = utils.parse_spatial_args(args, parse_transformation=False)
    view = utils.parse_view(args)
    add_registered_source(args.input_path, args.input_key, args.transformation,
                          args.root, args.dataset_name, args.name,
                          resolution, scale_factors, chunks,
                          method=args.method, menu_name=args.menu_name,
                          shape=args.shape, source_type=args.source_type, view=view,
                          add_default_table=bool(args.add_default_table),
                          tmp_folder=args.tmp_folder, target=args.target, max_jobs=args.max_jobs,
                          is_default_dataset=bool(args.is_default_dataset),
                          **utils.get_source_kwargs(args))
