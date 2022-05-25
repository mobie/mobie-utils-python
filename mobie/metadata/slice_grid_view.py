import json
import os
import warnings

import numpy as np
import elf.transformation as trafo_utils
import s3fs
from pybdv import metadata as bdv_metadata
from . import view_metadata as view_utils
from .dataset_metadata import read_dataset_metadata, write_dataset_metadata


# TODO refactor with the rest
def _load_bdv_metadata(dataset_folder, storage):
    xml_path = os.path.join(dataset_folder, storage["relativePath"])
    return xml_path


def _load_json_from_s3(address):
    server = "/".join(address.split("/")[:3])
    path = "/".join(address.split("/")[3:])
    try:
        fs = s3fs.S3FileSystem(anon=True, client_kwargs={"endpoint_url": server})
        store = s3fs.S3Map(root=path, s3=fs)
        attrs = store[".zattrs"]
    except Exception:
        return None
    attrs = json.loads(attrs.decode("utf-8"))
    return attrs


def _load_json_from_file(path):
    if not os.path.exists(path):
        return None
    with open(path) as f:
        attrs = json.load(f)
    return attrs


# TODO refactor with the rest
def _load_ome_zarr_metadata(dataset_folder, storage, data_format):
    if data_format == "ome.zarr":
        attrs_path = os.path.join(dataset_folder, storage["relativePath"], ".zattrs")
        attrs = _load_json_from_file(attrs_path)
    else:
        assert data_format == "ome.zarr.s3"
        address = storage["s3Address"]
        attrs = _load_json_from_s3(address)
    return None if attrs is None else attrs[0]["multiscales"]


# TODO refactor with the rest
def _load_image_metadata(image_data, dataset_folder):
    image_metadata = None
    for data_format, storage in image_data.items():
        if data_format.startswith("bdv"):
            image_metadata = _load_bdv_metadata(dataset_folder, storage)
        elif data_format.startswith("ome.zarr"):
            image_metadata = _load_ome_zarr_metadata(dataset_folder, storage, data_format)
        if image_metadata is not None:
            return data_format, image_metadata
    raise RuntimeError(f"Could not load the image metadata for {image_metadata}")


# TODO refactor into source_metadata.py, generally very useful!!!
def _load_shape(image_data, dataset_folder):
    data_format, image_metadata = _load_image_metadata(image_data, dataset_folder)
    if data_format.startswith("bdv"):
        shape = bdv_metadata.get_size(image_metadata, setup_id=0)
    elif data_format == "ome.zarr":
        dataset_path = image_metadata["datasets"][0]["path"]
        array_path = os.path.join(
            dataset_folder, image_data["storage"][data_format]["relativePath"], dataset_path, ".zarray"
        )
        array_metadata = _load_json_from_file(array_path)
        shape = array_metadata["shape"]
    elif data_format == "ome.zarr.s3":
        dataset_path = image_metadata["datasets"][0]["path"]
        address = image_data[data_format]["s3Address"]
        array_address = os.path.join(address, dataset_path, ".zarray")
        array_metadata = _load_json_from_s3(array_address)
        shape = array_metadata["shape"]
    else:
        raise ValueError(f"Unsupported data format {data_format}")
    return shape


def _bdv_transform_to_affine_matrix(transforms):
    assert isinstance(transforms, dict)
    transforms = list(transforms.values())
    # TODO do we need to pass the resolution here ????
    transforms = [trafo_utils.bdv_to_native(trafo) for trafo in transforms]
    # TODO is this the correct order of concatenation?
    transform = transforms[0]
    for trafo in transforms[1:]:
        transform = transform @ trafo
    return transform


# TODO refactor into elf.transformation
def _ome_zarr_transform_to_affine_matrix(transforms, version):
    supported_versions = ("0.4",)
    if version not in supported_versions:
        raise RuntimeError(f"Currently only support ome.zarr versions {supported_versions}, got {version}")
    scale, translation = None, None
    assert len(transforms) <= 2
    for trafo in transforms:
        trafo_type = trafo["type"]
        assert trafo_type in ("scale", "translation"), f"Expected scale or translation transform, got {trafo_type}"
        if trafo_type == "scale":
            scale = trafo["scale"]
        if trafo_type == "translation":
            translation = trafo["translation"]
    transform = trafo_utils.affine_matrix_3d(scale=scale, translation=translation)
    # TODO handle top-level trafo
    return transform


# load the transformation from the metadata of this source
# TODO refactor into source_metadta.py, generally very useful!!!
def _load_transformation(image_data, dataset_folder, to_affine_matrix=True):
    data_format, image_metadata = _load_image_metadata(image_data, dataset_folder)
    if data_format.startswith("bdv"):
        transform = bdv_metadata.get_affine(image_metadata, setup_id=0)
        if to_affine_matrix:
            transform = _bdv_transform_to_affine_matrix(transform)
    elif data_format.startswith("ome.zarr"):
        transform = image_metadata["datasets"][0]["coordinateTransformations"]
        version = image_metadata["version"]
        if to_affine_matrix:
            transform = _ome_zarr_transform_to_affine_matrix(transform, version)
    else:
        raise ValueError(f"Unsupported data format {data_format}")
    return transform


def _get_slice_grid(
    dataset_folder,
    dataset_metadata,
    source,
    n_slices,
    view_name,
    menu_name,
    initial_transforms,
    display_settings,
    is_exclusive,
):
    sources = dataset_metadata["sources"]
    views = dataset_metadata["views"]

    source_type, source_data = next(iter(sources[source].items()))
    if source_type != "image":
        raise ValueError(f"create_slice_grid is only supported for image sources, got {source_type}.")

    z_axis = 0
    shape = _load_shape(source_data["imageData"], dataset_folder)
    ax_len = shape[z_axis]
    # spacing_pixels = shape[z_axis] / n_slices
    if ax_len % n_slices != 0:
        msg = "Can't evenly split volume with length {ax_len} into {n_slices} slices.\n"
        msg += "The space between slices will be {spacing_pixels} pixels."
        warnings.warn(msg)

    # load the data transformation and compose it with the
    data_transform = _load_transformation(source_data["imageData"], dataset_folder)
    if initial_transforms is not None:
        # TODO
        raise NotImplementedError

    shape_vector = np.array(list(shape) + [1], dtype="float64")
    transformed_shape = data_transform @ shape_vector
    assert transformed_shape.shape == (4,)
    z_extent = shape_vector[0]
    z_spacing = z_extent / n_slices

    # compute the individual slice transformations (shifts)
    # to enable axis != 0 we would also need to do an axis rotation here
    source_transformations = []
    grid_sources = []
    for slice_id in range(n_slices):
        slice_trafo_params = trafo_utils.affine_matrix_3d(translation=[slice_id * z_spacing, 0.0, 0.0])
        # TODO do we need the resolution here?!
        slice_trafo_params = trafo_utils.native_to_bdv(slice_trafo_params)
        name_after_trafo = f"{source}_z{slice_id}"
        slice_trafo = view_utils.get_affine_source_transform(
            [source], slice_trafo_params, source_names_after_transform=[name_after_trafo]
        )
        source_transformations.append(slice_trafo)
        grid_sources.append(name_after_trafo)

    # add the grid transformation
    nested_grid_sources = [[src] for src in grid_sources]
    grid = view_utils.get_transformed_grid_source_transform(nested_grid_sources)
    source_transformations.append(grid)

    if display_settings is None:
        display_name = f"{source}-slice-grid"
        # if no display settings were passed, try to use the default view for this source
        try:
            reference_displays = views[source]["sourceDisplays"]
            assert len(reference_displays) == 1
            display_settings = reference_displays[0]
            assert "imageDisplay" in display_settings
            display_settings["imageDispaly"]["name"] = display_name
            display_settings["imageDispaly"]["sources"] = grid_sources
        # and if we don't have a default view, use the default imageSource settings
        except Exception:
            warnings.warn(f"Could not parse the display settings for {source}, using default settings")
            display_settings = view_utils.get_image_display(display_name, grid_sources)
    else:
        assert isinstance(display_settings, dict)
        # there are two options of passing display settings: either as full 'imageDisplay'
        # or as kwargs for 'get_image_display'. They need to be treated differently
        if "imageDisplay" in display_settings:
            display_name = display_settings["imageDisplay"]["name"]
        elif "name" in display_settings:
            display_name = display_settings["name"]
        else:
            display_name = f"{source}-slice-grid"

    new_view = view_utils.get_view([display_name], [source_type], [grid_sources], [display_settings],
                                   is_exclusive=is_exclusive, menu_name=menu_name,
                                   source_transforms=source_transformations)
    return new_view


# TODO should be unified with functionality from 'bookmark_metadata'
# (which is a bad name for this file, eventaully need to restrucure this for a 0.5.0 release)
def _write_view(dataset_folder, dataset_metadata, view_file, view_name, new_view, overwrite, return_view):
    # we don't write the view, but return it
    if return_view:
        return new_view

    if view_file is None:  # write to dataset metadata
        views = dataset_metadata["views"]
        view_source = dataset_folder
    else:
        if os.path.exists(view_file):
            with open(view_file, "r") as f:
                views = json.load(f)["views"]
        else:
            views = {}
        view_source = view_file

    if view_name in views:
        msg = f"The view {view_name} is alread present at {view_source}."
        if overwrite:
            warnings.warn(msg + " It will be over-written.")
        else:
            raise ValueError(msg)
    views[view_name] = new_view

    if view_file is None:
        dataset_metadata["views"] = views
        write_dataset_metadata(dataset_folder, dataset_metadata)
    else:
        with open(view_file, "w") as f:
            json.dump({"views": views}, f)


def create_slice_grid(
    dataset_folder,
    source,
    n_slices,
    view_name,
    menu_name,
    initial_transforms=None,
    display_settings=None,
    overwrite=False,
    view_file=None,
    is_exclusive=True,
    return_view=False,
):
    """Create a grid that aligns n slices of a source shifted so that each n-th slice is aligned with the origin.

    Arguments:
        dataset_folder [str] -
        source [str] -
        n_slices [int] -
        view_name [str] - name for the sliced view
        menu_name [str] - menu name for the sliced view
        initial_transforms [list[dict]] - list of transformations to be applied before slicing. (default: None)
        display_settings [dict] - display settings for the resluting view.
            By default will use the display settings of the default view for this source. (default: None)
        overwrite [bool] - whether to overwrite existing views (default: False)
        view_file [str] - file path for a view file to which the view should be saved.
            By default it will be saved in the dataset metadata. (default: None)
        is_exclusive [bool] - whether this is an exclusive view (default: True)
        return_view [bool] - return the generated view instead of serializing it (default: False)
    """
    dataset_metadata = read_dataset_metadata(dataset_folder)
    if source not in dataset_metadata["sources"]:
        raise ValueError(f"The source {source} could not be found in the dataset at {dataset_folder}.")
    sliced_view = _get_slice_grid(
        dataset_folder, dataset_metadata, source, n_slices,
        view_name, menu_name, initial_transforms, display_settings, is_exclusive
    )
    _write_view(
        dataset_folder, dataset_metadata, view_file, view_name, sliced_view, overwrite, return_view
    )


def create_slice_grid_with_reference_view(
    dataset_folder,
    source,
    reference_view,
    n_slices,
    view_name,
    menu_name=None,
    initial_transforms=None,
    display_settings=None,
    overwrite=False,
    view_file=None,
    is_exclusive=True,
    return_view=False,
):
    """Create a grid that aligns n slices of a source shifted so that each n-th slice is aligned with the origin,
    taking other settings from a reference view.

    The reference view will be used to derive initial transformation, display settigns and
    other view specific parameters, unless over-written by a more explicit parameter.

    Arguments:
        dataset_folder [str] -
        source [str] -
        reference_view [str] -
        n_slices [int] -
        view_name [str] - name for the sliced view.
        menu_name [str] - menu name for the sliced view.
            By default will be taken from the reference view (default: None).
        initial_transforms [list[dict]] - list of transformations to be applied before slicing.
            By default will be taken from the reference view. (default: None)
        display_settings [dict] - display settings for the resluting view.
            By default will be taken from the reference view (default: None)
        overwrite [bool] - whether to overwrite existing views (default: False)
        view_file [str] - file path for a view file to which the view should be saved.
            By default it will be saved in the dataset metadata. (default: None)
        is_exclusive [bool] - whether this is an exclusive view (default: True)
        return_view [bool] - return the generated view instead of serializing it (default: False)
    """
    dataset_metadata = read_dataset_metadata(dataset_folder)
    if source not in dataset_metadata["sources"]:
        raise ValueError(f"The source {source} could not be found in the dataset at {dataset_folder}.")

    # load the reference view and use it for default settings, transformations etc.
    views = dataset_metadata["views"]
    if reference_view not in views:
        raise ValueError("The reference view {reference_view} could not be found in the dataset at {dataset_folder}.")
    ref_view = views[reference_view]

    if menu_name is None:
        menu_name = ref_view["uiSelectionGroup"]

    if initial_transforms is None and "sourceTransforms" in ref_view:
        # get the transformations for this source from the reference view
        source_transforms = ref_view["sourceTranforms"]
        initial_transforms = []
        for trafo in source_transforms:
            trafo_type, trafo_params = next(iter(trafo))
            if trafo_type != "affine":
                raise ValueError(f"Only support reference views with affine transformations, got {trafo_type}")
            if source not in trafo_params["sources"]:
                continue
            initial_transforms.append(
                view_utils.get_affine_source_transform([source], trafo_params["parameters"],
                                                       timepoints=trafo_params.get("timepoints", None),
                                                       name=trafo_params.get("name", None))
            )

    if display_settings is None and "sourceDisplays" in ref_view:
        # get the display settings for this source from the reference view
        source_displays = ref_view["sourceDisplays"]
        for display in source_displays:
            if "imageDisplay" not in display:
                continue
            settings = next(iter(display.values()))
            if source in settings["sources"]:
                settings.pop("sources")
                settings["sources"] = [source]
                display_settings = view_utils.get_image_display(**settings)
                break

    sliced_view = _get_slice_grid(
        dataset_folder, dataset_metadata, source, n_slices, view_name,
        menu_name, initial_transforms, display_settings, is_exclusive
    )
    return _write_view(
        dataset_folder, dataset_metadata, view_file, view_name, sliced_view, overwrite, return_view
    )
