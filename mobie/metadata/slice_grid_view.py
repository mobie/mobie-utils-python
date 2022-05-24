import warnings
from elf.io import open_file

from . import view_metadata as view_utils
from .dataset_metadata import read_dataset_metadata, write_dataset_metadata


# TODO handle online sources and refactor this to somewhere more generic
# generally very useful!!!
def _load_shape(image_data):
    pass


def _load_transformation():
    pass


# load the scale for the image data. if axis is given only for that axis.
# use _load_transformation to get the full transformation from the iamge metadata
# TODO handle online sources and refactor this to somewhere more generic
# generally very useful!!!
def _load_scale(image_data, axis=None):
    pass


def create_slice_grid(dataset_folder, source, n_slices, view_name, axis=0, overwrite=False):
    """Create a grid that aligns n slices of a source shifted so that each n-th slice is aligned with the origin.

    Arguments:
        dataset_folder [str] -
        source [str] -
        n_slices [int] -
        view_name [str] -
        axis [int] -
        overwrite [bool] - whether to overwrite existing views (default: False)
    """
    if axis != 0:
        raise NotImplementedError("create_slice_grid is currently only supported along the z-axis (axis 0)")

    ds_meta = read_dataset_metadata(dataset_folder)
    sources = ds_meta["sources"]
    views = ds_meta["views"]

    if source not in sources:
        raise ValueError(f"The source {source} could not be found in the sources of the dataset at {dataset_folder}.")
    if view_name in views:
        msg = "The view {name} is alread present in the dataset at {dataset_folder}."
        if overwrite:
            warnings.warn(msg + " It will be over-written.")
        else:
            raise ValueError(msg)

    source_type, source_data = next(iter(sources[source].items()))
    if source_type != "imageSource":
        raise ValueError("create_slice_grid is only supported for image sources.")

    shape = _load_shape(source_data["imageData"])
    ax_len = shape[axis]
    spacing_pixels = shape[axis] / n_slices
    if ax_len % n_slices != 0:
        msg = "Can't evenly split volume with length {ax_len} into {n_slices} slices.\n"
        msg += "The space between slices will be {spacing_pixels} pixels."
        warnings.warn(msg)

    ax_scale = _load_scale(source_data["imageData"], axis)

    # compute the individual slice transformations (shifts)
    # to enable axis != 0 we would also need to do an axis rotation here
    source_transformations = []
    grid_sources = []
    for slice_id in range(n_slices):
        # TODO double check this!!!
        slice_shift = spacing_pixels / ax_scale
        slice_trafo_params = 11 * [0] + [slice_shift]
        name_after_trafo = f"{source}"
        slice_trafo = view_utils.get_affine_source_transform(
            [source], slice_trafo_params, source_names_after_transform=[name_after_trafo]
        )
        source_transformations.append(slice_trafo)
        grid_sources.append(name_after_trafo)

    # add the grid transformation
    grid = view_utils.get_transformed_grid_source_transform(grid_sources)
    source_transformations.append(grid)

    # TODO load the viewer setting for the default view for this source, otherwise use generic params
    # for the image display

    # TODO
    new_view = view_utils.get_view()
    ds_meta["views"][new_view] = new_view

    write_dataset_metadata(dataset_folder, ds_meta)
