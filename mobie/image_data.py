import multiprocessing
import os
import warnings
import shutil
from typing import Optional, Tuple, Union

import mobie.metadata as metadata
import mobie.utils as utils
import numpy as np
import pybdv.metadata as bdv_metadata
from elf.io import open_file
from mobie.import_data import import_image_data
from pybdv.util import absolute_to_relative_scale_factors, get_key, get_scale_factors


def _get_default_contrast_limits(input_path, input_key, int_to_uint):
    with open_file(input_path, "r") as f:
        dtype = f[input_key].dtype

    if np.issubdtype(dtype, np.integer):
        if int_to_uint:
            # NOTE: this may need the same dtype mapping as here to be robust:
            # https://github.com/constantinpape/cluster_tools/blob/master/cluster_tools/copy_volume/copy_volume.py#L21-L30
            # but this code should be refactored and improved!
            assert np.issubdtype(dtype, np.signedinteger)
            dtype = "u" + str(dtype)
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
    setup_ids=None,
    scale_factors=None,
    tmp_folder=None,
    target="local",
    max_jobs=multiprocessing.cpu_count(),
    is_default_dataset=False,
    description=None,
    trafos_for_mobie=None,
    move_data=False,
    int_to_uint=False
):
    """Add the image(s) specified in an bdv xml file and copy the metadata.
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
                  move_only=move_only, int_to_uint=int_to_uint)


# TODO support default arguments for scale factors and chunks
def add_image(input_path, input_key,
              root, dataset_name, image_name,
              resolution, scale_factors, chunks,
              file_format="ome.zarr", menu_name=None,
              tmp_folder=None, target="local",
              max_jobs=multiprocessing.cpu_count(),
              view=None, transformation=None,
              unit="micrometer",
              is_default_dataset=False,
              description=None,
              move_only=False,
              int_to_uint=False,
              channel=None,
              skip_add_to_dataset=False):
    """ Add an image source to a MoBIE dataset.

    Will create the dataset if it does not exist.

    Arguments:
        input_path [str, np.ndarray] - path to the data that should be added.
            This can also be a numpy array in order to save in memory data.
        input_key [str] - key to the data that should be added.
        root [str] - data root folder.
        dataset_name [str] - name of the dataset the image data should be added to.
        image_name [str] - name of the image data.
        resolution [list[float]] - resolution of the image data in micrometer.
        scale_factors [list[list[int]]] - scale factors used for down-sampling.
        chunks [list[int]] - chunks for the data.
        menu_name [str] - menu name for this source.
            If none is given will be created based on the image name. (default: None)
        file_format [str] - the file format used to store the data internally (default: ome.zarr)
        tmp_folder [str] - folder for temporary files (default: None)
        target [str] - computation target (default: "local")
        max_jobs [int] - number of jobs (default: number of cores)
        view [dict] - default view settings for this source (default: None)
        transformation [list or np.ndarray] - parameter for affine transformation
            applied to the data on the fly (default: None)
        unit [str] - physical unit of the coordinate system (default: micrometer)
        is_default_dataset [bool] - whether to set new dataset as default dataset.
            Only applies if the dataset is being created. (default: False)
        description [str] - description for this image (default: None)
        move_only [bool] - if input data is already in a MoBIE compatible format,
            just move it into the project directory. (default: False)
        int_to_uint [bool] - whether to convert signed to unsigned integer (default: False)
        channel [int] - the channel to load from the data.
            Currently only supported for the ome.zarr format (default: None)
        skip_add_to_dataset [bool] - Skip adding the source to the dataset after converting the image data.
            This should be used when calling `add_image` in parallel in order to avoid
            writing to dataset.json in parallel, which can cause issues. In this case the source needs to be added later
            , which can be done by calling this function again. (default: False)
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
        contrast_limits = _get_default_contrast_limits(input_path, input_key, int_to_uint)
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
        if int_to_uint:
            raise ValueError("Conversion of integer to unsigned integer is not possible with move_only")
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
                          int_to_uint=int_to_uint,
                          channel=channel)

    if transformation is not None:
        utils.update_transformation_parameter(image_metadata_path, transformation, file_format)

    if skip_add_to_dataset:
        return
    metadata.add_source_to_dataset(dataset_folder, "image", image_name, image_metadata_path,
                                   view=view, description=description, channel=channel)


def main():
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
