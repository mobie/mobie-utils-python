import warnings
from . import metadata as mobie_metadata
from .validation import validate_view_metadata


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
