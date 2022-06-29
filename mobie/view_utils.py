import argparse
import json
import os
import warnings

from . import metadata as mobie_metadata
from .validation import validate_view_metadata, validate_views, validate_with_schema

#
# view creation
#


def _create_view(
    sources, all_sources, display_settings,
    source_transforms, viewer_transform,
    display_group_names, menu_name,
    is_exclusive=True
):
    all_source_names = set(all_sources.keys())
    source_types = []
    for source_list in sources:

        invalid_source_names = list(set(source_list) - all_source_names)
        if invalid_source_names:
            raise ValueError(f"Invalid source names: {invalid_source_names}")

        this_source_types = list(set(
            [list(all_sources[source].keys())[0] for source in source_list]
        ))
        if len(this_source_types) > 1:
            raise ValueError(f"Inconsistent source types: {this_source_types}")
        source_types.append(this_source_types[0])

    if display_group_names is None:
        # 'unpack' display settings if needed, i.e. go from {"imageDisplay": {...}} to {...}
        # if the display settings are passed like this
        display_settings_unpacked = [
            next(iter(display.values()))
            if (len(display) == 1 and "imageDisplay" in display or "segmentationDisplay" in display)
            else display
            for display in display_settings
        ]
        display_group_names = [
            display.get("name", f"{source_type}-group-{i}")
            for i, (source_type, display) in enumerate(zip(source_types, display_settings_unpacked))
        ]

    view = mobie_metadata.get_view(
        display_group_names, source_types,
        sources, display_settings,
        is_exclusive=is_exclusive,
        menu_name=menu_name,
        source_transforms=source_transforms,
        viewer_transform=viewer_transform
    )
    return view


def _write_view(dataset_folder, view_file, view_name, view, overwrite, return_view):
    # we don't write the view, but return it
    if return_view:
        return view
    # write the view to the dataset
    elif view_file is None:
        mobie_metadata.add_view_to_dataset(dataset_folder, view_name, view, overwrite=overwrite)
        return

    # write the view to an external view file
    if os.path.exists(view_file):
        with open(view_file, "r") as f:
            views = json.load(f)["views"]
    else:
        views = {}

    if view_name in views:
        msg = f"The view {view_name} is alread present in {view_file}."
        if overwrite:
            warnings.warn(msg + " It will be over-written.")
        else:
            raise ValueError(msg)

    views[view_name] = view
    with open(view_file, "w") as f:
        json.dump({"views": views}, f)


def create_view(
    dataset_folder, view_name,
    sources, display_settings,
    source_transforms=None,
    viewer_transform=None,
    display_group_names=None,
    menu_name="bookmark",
    is_exclusive=True,
    overwrite=False,
    view_file=None,
    return_view=False,
):
    """Add or update a view in dataset.json:views.

    Views can reproduce any given viewer state.

    Arguments:
        dataset_folder [str] - path to the dataset folder
        view_name [str] - name of the view
        sources [list[list[str]]] - nested list of sources for this view.
            Each inner list contains the sources for one of the source displays.
        display_settings [list[dict]] - List of display settings for the source displays.
        source_transforms [list[dict]] - List of source transformations. (default: None)
        viewer_transform [dict] - the viewer transformation. (default:None)
        display_group_names [list[str]] - the names for the source displays (default: None)
        menu_name [str] - name for the menu where this view will be saved (default: bookmark)
        is_exclusive [bool] - whether the view is exclusive (default: True)
        overwrite [bool] - whether to overwrite existing views (default: False)
        view_file [str] - name of the view file where this view should be saved.
            By default it will be saved directly in the dataset metadata (default: None)
        return_view [bool] - whether to return the created view instead of
            saving it to the dataset or to an external view file (default: False)
    """
    dataset_metadata = mobie_metadata.read_dataset_metadata(dataset_folder)
    all_sources = dataset_metadata["sources"]
    view = _create_view(sources, all_sources, display_settings,
                        source_transforms, viewer_transform,
                        display_group_names, menu_name=menu_name,
                        is_exclusive=is_exclusive)
    validate_with_schema(view, "view")
    return _write_view(dataset_folder, view_file, view_name, view,
                       overwrite=overwrite, return_view=return_view)


def create_grid_view(
    dataset_folder, view_name, sources,
    table_folder=None,
    display_groups=None,
    display_group_settings=None,
    positions=None,
    use_transformed_grid=True,
    menu_name="bookmark",
    overwrite=False,
    view_file=None,
    return_view=False,
):
    """ Add or update a grid view.

    Arguments:
        dataset_folder [str] - path to the dataset folder
        view_name [str] - name of the view
        sources [list[list[str]]] - sources to be arranged in the grid.
            The sources need to be passed as a nested list, where each inner list contains the
            sources for one of the grid positions.
        table_folder [str] - path to the table folder, relative to the dataset folder (default: None)
        display_groups [dict[str, str] - the display groups in this view. Needs to be a map from source name
            to the name of the display group for this sources. By default all sources will end up in their own
            display group with the settings for the default view of the source (default: None)
        display_group_settings [dict[str, dict]] - the settings for the display groups in the view.
            The keys must be the values of the display_groups parameter (default: None)
        positions [list[list[int]]] - list of explicit grid positions.
            If given, must have the same length as sources, the inner lists must contain two values,
            corresponding to the 2d grid positions (default: None)
        use_transformed_grid [bool] - whether to use a transformed or merged grid (default: True)
        menu_name [str] - name of the menu from whil this view can be selected (default: bookmark)
        overwrite [bool] - whether to overwrite existing view (default: False)
        view_file [str] - name of the view file where this view should be saved.
            By default it will be saved directly in the dataset metadata (default: None)
        return_view [bool] - whether to return the created view instead of
            saving it to the dataset or to an external view file (default: False)
    """
    assert all(source_list for source_list in sources)
    view = mobie_metadata.get_grid_view(
        dataset_folder, view_name, sources, menu_name=menu_name,
        table_folder=table_folder, display_groups=display_groups,
        display_group_settings=display_group_settings, positions=positions,
        use_transformed_grid=use_transformed_grid,
    )
    validate_with_schema(view, "view")
    return _write_view(dataset_folder, view_file, view_name, view,
                       overwrite=overwrite, return_view=return_view)


#
# view merging / combination
#


def merge_view_file(dataset_folder, view_file, overwrite=False):
    """Merge views from a view file into the views of a dataset.

    Arguments:
        dataset_folder [str] - path to the dataset_folder
        view_file [str] - path to the view file
        overwrite [bool] - whether to over existing views in the dataset (default: False)
    """
    validate_views(view_file)

    metadata = mobie_metadata.read_dataset_metadata(dataset_folder)
    ds_views = metadata["views"]

    with open(view_file) as f:
        views = json.load(f)["views"]

    duplicate_views = [name for name in views if name in ds_views]
    if duplicate_views:
        msg = f"Duplicate views {duplicate_views} in view file {view_file} and dataset {dataset_folder}"
        if overwrite:
            raise RuntimeError(msg)
        else:
            warnings.warn(msg)
    ds_views.update(views)

    metadata["views"] = ds_views
    mobie_metadata.write_dataset_metadata(dataset_folder, metadata)


def combine_views(dataset_folder, view_names, new_view_name, menu_name, keep_original_views=True):
    """Combine several views in a dataset.

    Arguments:
        dataset_folder [str] - path to the dataset folder
        view_names [list[str] or tuple[str]] - names of the views to be combined
        new_view_name [str] - name of the combined view
        menu_name [str] - menu name of the combined view
        keep_original_views [bool] - whether to keep the original views (default: True)
    """
    warnings.warn(
        "combine_views is experimental and will currently only work for relatively simple views."
        "The result for more complex views may be incorrect without raising any errors."
    )
    assert isinstance(view_names, (list, tuple))
    metadata = mobie_metadata.read_dataset_metadata(dataset_folder)
    views = metadata["views"]
    if not all(name in views for name in view_names):
        raise ValueError(f"Can't find all view names: {view_names} in the dataset at {dataset_folder}")

    is_exclusive = None
    source_displays = []
    source_transforms = []
    for name in view_names:
        this_view = views[name]
        # handle viewer transforms?
        if "viewerTransform" in this_view:
            raise RuntimeError("Views with a viewerTransform cannot be combined")
        this_exclusive = this_view["isExclusive"]
        if is_exclusive is None:
            is_exclusive = this_exclusive
        elif is_exclusive != this_exclusive:
            raise RuntimeError("Views with different values for 'isExclusive' cannot be combined")
        source_displays.extend(this_view.get("sourceDisplays", []))
        source_transforms.extend(this_view.get("sourceTransforms", []))

    new_view = {
        "sourceDisplays": source_displays, "sourceTransforms": source_transforms,
        "uiSelectionGroup": menu_name, "isExclusive": is_exclusive
    }
    validate_view_metadata(new_view)
    views[new_view_name] = new_view

    if not keep_original_views:
        views = {k: v for k, v in views.items() if k not in view_names}

    metadata["views"] = views
    mobie_metadata.write_dataset_metadata(dataset_folder, metadata)


def main():
    parser = argparse.ArgumentParser("Merge views from a view file into the views of a dataset.")
    parser.add_argument("-d", "--dataset", help="Path to the dataset folder", required=True)
    parser.add_argument("-v", "--views", help="Path to the view file", required=True)
    parser.add_argument("-o", "--overwrite", help="Whether to overwrite existing views", default=0, type=int)
    args = parser.parse_args()
    merge_view_file(args.dataset, args.views, bool(args.overwrite))
