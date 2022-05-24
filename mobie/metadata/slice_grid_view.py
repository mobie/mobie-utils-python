import warnings

from . import view_metadata as view_utils
from .dataset_metadata import read_dataset_metadata, write_dataset_metadata


# TODO handle online sources, refactor into source_metadata.py
# generally very useful!!!
def _load_shape(image_data):
    pass


# load the transformation from the metadata of this source
# TODO handle online sources, refactor into source_metadta.py
# generally very useful!!!
def _load_transformation(image_data):
    pass


def _get_slice_grid(
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
    if source_type != "imageSource":
        raise ValueError("create_slice_grid is only supported for image sources.")

    z_axis = 0
    shape = _load_shape(source_data["imageData"])
    ax_len = shape[z_axis]
    # spacing_pixels = shape[z_axis] / n_slices
    if ax_len % n_slices != 0:
        msg = "Can't evenly split volume with length {ax_len} into {n_slices} slices.\n"
        msg += "The space between slices will be {spacing_pixels} pixels."
        warnings.warn(msg)

    # load the data transformation and compose it with the
    data_transform = _load_transformation(source_data["imageData"])
    if initial_transforms is not None:
        pass  # TODO

    # compute the individual slice transformations (shifts)
    # to enable axis != 0 we would also need to do an axis rotation here
    source_transformations = []
    grid_sources = []
    for slice_id in range(n_slices):
        # TODO need to bring 'data_transform' and 'shape' into correct shapes and types so that this works;
        # will need some functionaliy from elf.transformation for this.
        # and need to find correct shift for this slice
        slice_trafo_params = data_transform @ shape
        name_after_trafo = f"{source}_z{slice_id}"
        slice_trafo = view_utils.get_affine_source_transform(
            [source], slice_trafo_params, source_names_after_transform=[name_after_trafo]
        )
        source_transformations.append(slice_trafo)
        grid_sources.append(name_after_trafo)

    # add the grid transformation
    grid = view_utils.get_transformed_grid_source_transform(grid_sources)
    source_transformations.append(grid)

    # TODO
    if display_settings is None:
        # if no display settings were passed, use them from the default view for this source
        if source in views:
            pass
        # and if we don't have a default view, use the default imageSource settings
        else:
            pass

    new_view = view_utils.get_view([display_name], [source_type], [grid_sources], [display_settings],
                                   is_exclusive=is_exclusive, menu_name=menu_name,
                                   source_transforms=source_transformations)
    return new_view


# TODO should be unified with functionality from 'bookmark_metadata'
# (which is a bad name for this file, eventaully need to restrucure this for a 0.5.0 release)
def _write_view(dataset_folder, dataset_metadata, view_file, view_name, new_view, overwrite):
    # TODO write to view_file instead if it is not None
    if view_name in dataset_metadata["views"]:
        msg = "The view {name} is alread present in the dataset at {dataset_folder}."
        if overwrite:
            warnings.warn(msg + " It will be over-written.")
        else:
            raise ValueError(msg)
    dataset_metadata["views"][view_name] = new_view
    write_dataset_metadata(dataset_folder, dataset_metadata)


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
    """
    dataset_metadata = read_dataset_metadata(dataset_folder)
    if source not in dataset_metadata["sources"]:
        raise ValueError(f"The source {source} could not be found in the dataset at {dataset_folder}.")
    sliced_view = _get_slice_grid(
        dataset_metadata, source, n_slices, view_name, menu_name, initial_transforms, display_settings, is_exclusive
    )
    _write_view(dataset_folder, dataset_metadata, view_file, view_name, sliced_view, overwrite)


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

    if initial_transforms is None:
        # TODO get transforms for this source from the reference view
        pass

    if display_settings is None:
        # TODO get display settings for this source from the reference view
        pass

    sliced_view = _get_slice_grid(
        dataset_metadata, source, n_slices, view_name, menu_name, initial_transforms, display_settings, is_exclusive
    )
    _write_view(dataset_folder, dataset_metadata, view_file, view_name, sliced_view, overwrite)
