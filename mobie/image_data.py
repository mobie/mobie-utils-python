"""Import intensity image data into a MoBIE project.
"""

import multiprocessing
import os
import warnings
import shutil
from typing import Dict, List, Optional, Sequence, Tuple, Union

import mobie.metadata as metadata
import mobie.utils as utils
import numpy as np
import pybdv.metadata as bdv_metadata
import tifffile
from bioimage_py import open_source
from elf.io import open_file
from mobie.import_data import import_image_data
from pybdv.util import absolute_to_relative_scale_factors, get_key, get_scale_factors


def _get_default_contrast_limits(input_path, input_key, use_memmap=False):
    if use_memmap:
        dtype = tifffile.memmap(input_path).dtype
    else:
        # read the dtype via bioimage-py so all of its supported input formats work (e.g. mrc /
        # nifti, and single-file inputs with input_key=None, which elf's f[None] could not handle).
        src = open_source(input_path, input_key) if input_key else open_source(input_path)
        dtype = src.dtype

    if np.issubdtype(dtype, np.integer):
        contrast_limits = [np.iinfo(dtype).min, np.iinfo(dtype).max]
    elif np.issubdtype(dtype, np.floating):
        contrast_limits = [0.0, 1.0]
    else:
        contrast_limits = [0.0, 255.0]
        msg = f"Default contrast limits for dtype={dtype} are not available, setting them to {contrast_limits}"
        warnings.warn(msg)

    return contrast_limits


def _view_and_trafo_from_xml(xml_path, setup_id, timepoint, source_name, menu_name, trafos_for_mobie):

    def to_color(bdv_color):
        color = bdv_color.split()
        return f"r={color[0]},g={color[1]},b={color[2]},a={color[3]}"

    def to_blending_mode(bdv_blending):
        if bdv_blending == "Average":
            warnings.warn("Blending mode average is not supported by MoBIE, using sum instead.")
            mode = "sum"
        elif bdv_blending == "Sum":
            mode = "sum"
        else:
            raise RuntimeError(f"Bdv blending mode {bdv_blending} is not supported")
        return mode

    # try to parse the display settings from bdv attributes
    attributes = bdv_metadata.get_attributes(xml_path, setup_id)
    bdv_settings = attributes.get("displaysettings", None)
    if bdv_settings is None:
        display_settings = {}
    else:
        display_settings = {
            "contrastLimits": [bdv_settings["min"], bdv_settings["max"]],
            "color": to_color(bdv_settings["color"])
        }
        bdv_blending = bdv_settings.get("Projection_Mode", None)
        if bdv_blending is not None:
            display_settings["blendingMode"] = to_blending_mode(bdv_blending)
    display_settings = [display_settings]

    # get the transforms and divide them into mobie and bdv metadata
    bdv_trafos = bdv_metadata.get_affine(xml_path, setup_id, timepoint)
    transforms = {}  # the transforms to be written into bdv metadata
    mobie_transforms = []  # the transforms to be written into mobie metadata
    for trafo_name, params in bdv_trafos.items():
        if trafos_for_mobie is not None and trafo_name in trafos_for_mobie:
            mobie_transforms.append(
                {"affine": {"parameters": params, "sources": [source_name], "name": trafo_name}}
            )
        else:
            transforms[trafo_name] = params

    menu_name = "images" if menu_name is None else menu_name
    view = metadata.get_view([source_name], ["image"], [[source_name]], display_settings,
                             is_exclusive=False, menu_name=menu_name, source_transforms=mobie_transforms)
    return view, transforms


# TODO support multiple timepoints
def add_bdv_image(
    xml_path: Union[str, os.PathLike],
    root: str,
    dataset_name: str,
    image_name: Optional[Union[str, Tuple[str]]] = None,
    file_format: str = "bdv.n5",
    menu_name: Optional[str] = None,
    setup_ids: Optional[Sequence[int]] = None,
    scale_factors: Optional[Sequence[Sequence[int]]] = None,
    tmp_folder: Optional[str] = None,
    target: str = "local",
    max_jobs: int = multiprocessing.cpu_count(),
    is_default_dataset: bool = False,
    description: Optional[str] = None,
    trafos_for_mobie: Optional[Union[List[float], np.ndarray]] = None,
    move_data: bool = False,
) -> None:
    """Add the image(s) specified in an bdv xml file and copy the metadata.

    Args:
        input_path: Path to the bdv xml file
        root: Root folder of the MoBIE project.
        dataset_name: Name of the dataset the image data is added to.
        image_name: Name of the image data in MoBIE. If not given will derive the name from BDV metadata.
        file_format: The file format used to store the data internally.
        menu_name: Menu name for this source.
            If none is given will be created based on the image name.
        setup_ids: The setup ids to extract from the BDV data.
            If not given, then all available setup ids will be extracted.
        scale_factors: Scale factors used for down-sampling.
        tmp_folder: Folder for temporary files.
        target: The computation target.
        max_jobs: The maximum number of jobs for parallelization.
        is_default_dataset: Whether to set new dataset as default dataset.
            Only applies if the dataset is being created.
        description: Description for this image.
        trafos_for_mobie: Additional transformations.
        move_data: If input data is already in a MoBIE compatible format, just move it into the project directory.
    """
    # find how many timepoints we have
    t_start, t_stop = bdv_metadata.get_time_range(xml_path)
    if t_stop > t_start:
        raise NotImplementedError("Only a single timepoint is currently supported.")

    # get the setup ids and check that image_name is compatible
    if setup_ids is None:
        setup_ids = bdv_metadata.get_setup_ids(xml_path)
    else:
        all_setup_ids = bdv_metadata.get_setup_ids(xml_path)
        for setup_id in setup_ids:
            assert setup_id in all_setup_ids

    if image_name is None:
        image_name = [None] * len(setup_ids)
    else:
        if isinstance(image_name, str):
            image_name = [image_name]

    assert len(image_name) == len(setup_ids), f"{image_name}, {setup_ids}"

    data_path = bdv_metadata.get_data_path(xml_path, return_absolute_path=True)

    # get the key for the input data format
    input_format = bdv_metadata.get_bdv_format(xml_path)

    move_only = False
    if move_data:
        if input_format == file_format:
            move_only = True
        else:
            print("Different input format than target format. Will convert data instead of moving it.")

        if len(setup_ids) > 1:
            move_only = False
            print("Cannot move XML with multiple setups. Will convert data instead of moving it.")

    for setup_id, name in zip(setup_ids, image_name):
        input_key = get_key(input_format == "bdv.hdf5", timepoint=t_start, setup_id=setup_id, scale=0)

        # get the resolution, scale_factors, chunks and unit
        resolution = bdv_metadata.get_resolution(xml_path, setup_id)
        if scale_factors is None:
            scale_factors = get_scale_factors(data_path, setup_id)
            scale_factors = absolute_to_relative_scale_factors(scale_factors)[1:]
        with open_file(data_path, "r") as f:
            chunks = f[input_key].chunks
        unit = bdv_metadata.get_unit(xml_path, setup_id)

        # get the name of this source
        if name is None:
            name = bdv_metadata.get_name(xml_path, setup_id)

        # get the view (=MoBIE metadata) and transformation (=bdv metadata)
        # from the input bdv metadata
        view, transformation = _view_and_trafo_from_xml(xml_path, setup_id, t_start,
                                                        name, menu_name, trafos_for_mobie)

        tmp_folder_ = None if tmp_folder is None else f"{tmp_folder}_{name}"
        add_image(data_path, input_key, root, dataset_name,
                  image_name=name, resolution=resolution, scale_factors=scale_factors,
                  chunks=chunks, file_format=file_format, menu_name=menu_name,
                  tmp_folder=tmp_folder_, target=target, max_jobs=max_jobs,
                  unit=unit, view=view, transformation=transformation,
                  is_default_dataset=is_default_dataset, description=description,
                  move_only=move_only)


# TODO support default arguments for scale factors and chunks
def add_image(
    input_path: Union[str, np.ndarray],
    input_key: Optional[str],
    root: str,
    dataset_name: str,
    image_name: str,
    resolution: Sequence[float],
    scale_factors: List[List[int]],
    chunks: Sequence[int],
    file_format: str = "ome.zarr",
    menu_name: Optional[str] = None,
    tmp_folder: Optional[str] = None,
    target: str = "local",
    max_jobs: int = multiprocessing.cpu_count(),
    view: Optional[Dict] = None,
    transformation: Optional[Union[List[float], np.ndarray]] = None,
    unit: str = "micrometer",
    is_default_dataset: bool = False,
    description: Optional[str] = None,
    move_only: bool = False,
    channel: Optional[int] = None,
    skip_add_to_dataset: bool = False,
    use_memmap: bool = False,
) -> None:
    """Add an image source to a MoBIE dataset.

    Will create the dataset if it does not exist.

    Args:
        input_path: Path to the data to add.
            This can also be a numpy array for in memory data.
        input_key: Key of the data to add, corresponding to the internal path
            in hdf5/zarr/n5 etc. Set to None for adding data stored in a tif file.
        root: Root folder of the MoBIE project.
        dataset_name: Name of the dataset the image data is added to.
        image_name: Name of the image data in MoBIE.
        resolution: Resolution of the image data in micrometer.
        scale_factors: Scale factors used for down-sampling.
        chunks: Chunks for the data.
        menu_name: Menu name for this source.
            If none is given will be created based on the image name.
        file_format: The file format used to store the data internally.
        tmp_folder: Folder for temporary files.
        target: The computation target.
        max_jobs: The maximum number of jobs for parallelization.
        view: Default view settings for this source.
        transformation: Parameter for affine transformation applied to the data on the fly.
        unit: The physical unit of the coordinate system.
        is_default_dataset: Whether to set new dataset as default dataset.
            Only applies if the dataset is being created.
        description: Description for this image.
        move_only: If input data is already in a MoBIE compatible format, just move it into the project directory.
        channel: The channel to load from the data. Currently only supported for the ome.zarr format.
        skip_add_to_dataset: Skip adding the source to the dataset after converting the image data.
            This should be used when calling `add_image` in parallel in order to avoid
            writing to dataset.json in parallel, which can cause issues.
            In this case the source needs to be added later, e.g. by calling this function again.
        use_memmap: Whether to use memmap for loading the input data.
            This option is only supported for inputs in tif file format that can be loaded via `tifffile.memmap`.
            This does not work for images that are compressed or have an otherwise non-standard format.
    """
    # TODO add 'setup_id' to the json schema for bdv formats to also support it there
    if channel is not None and file_format != "ome.zarr":
        raise NotImplementedError("Channel setting is currently only supported for ome.zarr")

    tmp_folder = f"tmp_{dataset_name}_{image_name}" if tmp_folder is None else tmp_folder
    if isinstance(input_path, np.ndarray):
        input_path, input_key = utils.save_temp_input(input_path, tmp_folder, image_name)

    # set default contrast_limits if we don't have a view
    # or if the passed view doesn't hav contrast limits
    if view is None or "contrastLimits" not in view.get("sourceDisplays", [{}])[0].get("imageDisplay", {}):
        contrast_limits = _get_default_contrast_limits(input_path, input_key, use_memmap=use_memmap)
    else:
        contrast_limits = None
    view = utils.require_dataset_and_view(root, dataset_name, file_format,
                                          source_type="image", source_name=image_name,
                                          menu_name=menu_name, view=view,
                                          is_default_dataset=is_default_dataset,
                                          contrast_limits=contrast_limits,
                                          description=description)

    dataset_folder = os.path.join(root, dataset_name)

    # import the image data and add the metadata
    data_path, image_metadata_path = utils.get_internal_paths(dataset_folder, file_format, image_name)

    if move_only:
        shutil.move(input_path, data_path)
        if "bdv." in file_format:
            shutil.move(os.path.splitext(input_path)[0]+".xml", image_metadata_path)

    else:
        import_image_data(input_path, input_key, data_path,
                          resolution, scale_factors, chunks,
                          tmp_folder=tmp_folder, target=target,
                          max_jobs=max_jobs, unit=unit,
                          source_name=image_name,
                          file_format=file_format,
                          channel=channel)

    if transformation is not None:
        utils.update_transformation_parameter(image_metadata_path, transformation, file_format)

    if skip_add_to_dataset:
        return
    metadata.add_source_to_dataset(dataset_folder, "image", image_name, image_metadata_path,
                                   view=view, description=description, channel=channel)


def main():
    """@private
    """
    description = """Add image data to MoBIE dataset.
                     Initialize the dataset if it does not exist."""
    parser = utils.get_base_parser(description)
    args = parser.parse_args()

    resolution, scale_factors, chunks, transformation = utils.parse_spatial_args(args)
    view = utils.parse_view(args)
    add_image(args.input_path, args.input_key,
              args.root, args.dataset_name, args.name,
              resolution=resolution, scale_factors=scale_factors, chunks=chunks,
              view=view, menu_name=args.menu_name,
              tmp_folder=args.tmp_folder, target=args.target, max_jobs=args.max_jobs,
              is_default_dataset=bool(args.is_default_dataset),
              transformation=transformation, unit=args.unit)
