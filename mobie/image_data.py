import multiprocessing
import os
import warnings
import shutil

import mobie.metadata as metadata
import mobie.utils as utils
import pybdv.metadata as bdv_metadata
from elf.io import open_file
from mobie.import_data import import_image_data
from mobie.xml_utils import update_transformation_parameter
from pybdv.util import absolute_to_relative_scale_factors, get_key, get_scale_factors


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
def add_bdv_image(xml_path, root, dataset_name,
                  image_name=None, file_format="bdv.n5", menu_name=None, scale_factors=None,
                  tmp_folder=None, target="local", max_jobs=multiprocessing.cpu_count(),
                  is_default_dataset=False, description=None, trafos_for_mobie=None, move_data=False):
    """Add the image(s) specified in an bdv xml file and copy the metadata.
    """
    # find how many timepoints we have
    t_start, t_stop = bdv_metadata.get_time_range(xml_path)
    if t_stop > t_start:
        raise NotImplementedError("Only a single timepoint is currently supported.")

    # get the setup ids and check that image_name is compatible
    setup_ids = bdv_metadata.get_setup_ids(xml_path)

    if image_name is None:
        image_name = [None] * len(setup_ids)
    else:
        if isinstance(image_name, str):
            image_name = [image_name]

    assert len(image_name) == len(setup_ids)

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
def add_image(input_path, input_key,
              root, dataset_name, image_name,
              resolution, scale_factors, chunks,
              file_format="bdv.n5", menu_name=None,
              tmp_folder=None, target="local",
              max_jobs=multiprocessing.cpu_count(),
              view=None, transformation=None,
              unit="micrometer",
              is_default_dataset=False,
              description=None,
              move_only=False,
              int_to_uint=False):
    """ Add an image source to a MoBIE dataset.

    Will create the dataset if it does not exist.

    Arguments:
        input_path [str] - path to the data that should be added.
        input_key [str] - key to the data that should be added.
        root [str] - data root folder.
        dataset_name [str] - name of the dataset the image data should be added to.
        image_name [str] - name of the image data.
        resolution [list[float]] - resolution of the segmentation in micrometer.
        scale_factors [list[list[int]]] - scale factors used for down-sampling.
        chunks [list[int]] - chunks for the data.
        menu_name [str] - menu name for this source.
            If none is given will be created based on the image name. (default: None)
        file_format [str] - the file format used to store the data internally (default: bdv.n5)
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
            just move it into the project directory.
    """
    view = utils.require_dataset_and_view(root, dataset_name, file_format,
                                          source_type="image", source_name=image_name,
                                          menu_name=menu_name, view=view,
                                          is_default_dataset=is_default_dataset)

    dataset_folder = os.path.join(root, dataset_name)
    tmp_folder = f"tmp_{dataset_name}_{image_name}" if tmp_folder is None else tmp_folder

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
                          int_to_uint=int_to_uint)

    metadata.add_source_to_dataset(dataset_folder, "image", image_name, image_metadata_path,
                                   view=view, description=description)

    if transformation is not None:
        update_transformation_parameter(image_metadata_path, transformation)


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
