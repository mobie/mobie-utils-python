import json
import os
from shutil import rmtree

from pybdv import metadata as bdv_metadata
from .metadata import read_dataset_metadata, write_dataset_metadata


def _remove_image_data(storage_type, path):
    if storage_type.startswith("bdv"):  # bdv data: remove xml and data
        data_path = bdv_metadata.get_data_path(path, return_absolute_path=True)
        os.remove(path)
        rmtree(data_path) if storage_type.endswith("n5") else os.remove(data_path)
    else:  # ome.zarr data, can just rmtree
        rmtree(path)


def _remove_name_in_view(view, name):

    def update_sources(sources):
        return [source_name for source_name in sources if source_name != name]

    def update_nested_sources(nested_sources):
        new_nested_sources = []
        for source_list in nested_sources:
            new_source_list = [source_name for source_name in source_list if source_name != name]
            if not new_source_list:
                continue
            new_nested_sources.append(new_source_list)
        return new_nested_sources

    displays = view.get("sourceDisplays")
    if displays:
        new_displays = []
        for this_display in displays:
            # all displays have the source field
            display_type, this_display = next(iter(this_display.items()))
            new_sources = update_sources(this_display["sources"])
            if not new_sources:
                continue
            this_display["sources"] = new_sources
            new_displays.append({display_type: this_display})
        # if we don't have any source displays left then discard this view
        if not new_displays:
            return
        view["sourceDisplays"] = new_displays

    transforms = view.get("sourceTransforms")
    if transforms:
        new_transforms = []

        for transform in transforms:
            transform_type, transform = next(iter(transform.items()))
            # transforms have the sources or nestedSources field
            if "sources" in transform:
                new_sources = update_sources(transform["sources"])
                if not new_sources:
                    continue
                transform["sources"] = new_sources
            elif "nestedSources" in transform:
                new_sources = update_nested_sources(transform["nestedSources"])
                if not new_sources:
                    continue
                transform["nestedSources"] = new_sources

            new_transforms.append({transform_type: transform})

        if new_transforms:
            view["sourceTransforms"] = new_transforms

    return view


def remove_source(dataset_folder, name, remove_data=False):
    """Remove a source in the dataset.

    NOTE: this only works for local projects, i.e. projects that have not been
    uploaded to s3 yet.
    """
    dataset_metadata = read_dataset_metadata(dataset_folder)
    sources = dataset_metadata["sources"]

    # update the source metadata
    source_metadata = sources.pop(name, None)
    if source_metadata is None:
        raise ValueError(f"The source {name} is not present in the dataset at {dataset_folder}")
    source_type, source_metadata = next(iter(source_metadata.items()))

    # sources with image data: remove if remove_data is True
    if source_type in ("image", "segmentation"):
        storage = source_metadata["imageData"]
        if any("s3" in storage_type for storage_type in storage):
            raise ValueError("Removing sources that have been uploaded to s3 is not possible.")
        if remove_data:
            for storage_type, rel_path in storage.items():
                rel_path = rel_path["relativePath"]
                _remove_image_data(storage_type, os.path.join(dataset_folder, rel_path))

    # sources with segmentation data: remove if remove_data is True
    if remove_data and source_type in ("segmentation",):
        tab_data = source_metadata.get("tableData")
        if tab_data:
            rmtree(os.path.join(dataset_folder, tab_data["tsv"]["relativePath"]))

    dataset_metadata["sources"] = sources

    # go through all views and remove the source there
    # note that some views might become invalid after this operation and will be removed
    new_views = {}
    views = dataset_metadata["views"]
    for view_name, view in views.items():
        view = _remove_name_in_view(view, name)
        if view:
            new_views[view_name] = view
    dataset_metadata["views"] = new_views

    write_dataset_metadata(dataset_folder, dataset_metadata)


def _replace_name_in_data(storage_type, path, new_name):
    if "bdv" in storage_type:
        bdv_metadata.write_name(path, setup_id=0, name=new_name)
    elif "ome.zarr" in storage_type:
        attrs_path = os.path.join(path, ".zattrs")
        with open(attrs_path, "r") as f:
            attrs = json.load(f)
        attrs["multiscales"][0]["name"] = new_name
    else:
        raise ValueError(f"Invalid storage type {storage_type} for name replacement.")


def _replace_name_in_view(view, old_name, new_name):

    def update_sources(sources):
        return [new_name if name == old_name else name for name in sources]

    def update_nested_sources(nested_sources):
        return [
            [new_name if name == old_name else name for name in sources]
            for sources in nested_sources
        ]

    displays = view.get("sourceDisplays")
    if displays:
        new_displays = []
        for this_display in displays:
            # all displays have the source field
            display_type, this_display = next(iter(this_display.items()))
            this_display["sources"] = update_sources(this_display["sources"])
            new_displays.append({display_type: this_display})
        view["sourceDisplays"] = new_displays

    transforms = view.get("sourceTransforms")
    if transforms:
        new_transforms = []
        for transform in transforms:
            transform_type, transform = next(iter(transform.items()))
            # transforms have the sources or nestedSources field
            if "sources" in transform:
                transform["sources"] = update_sources(transform["sources"])
            elif "nestedSources" in transform:
                transform["nestedSources"] = update_nested_sources(transform["nestedSources"])
            new_transforms.append({transform_type: transform})
        view["sourceTransforms"] = new_transforms

    return view


def rename_source(dataset_folder, old_name, new_name):
    """Rename a source in the dataset.

    NOTE: this only works for local projects, i.e. projects that have not been
    uploaded to s3 yet.
    """
    dataset_metadata = read_dataset_metadata(dataset_folder)
    sources = dataset_metadata["sources"]

    # update the source metadata
    source_metadata = sources.pop(old_name, None)
    if source_metadata is None:
        raise ValueError(f"The source {old_name} is not present in the dataset at {dataset_folder}")
    source_type, source_metadata = next(iter(source_metadata.items()))

    # sources with image data, where we need to change the name in the image data
    if source_type in ("image", "segmentation"):
        storage = source_metadata["imageData"]
        for storage_type, rel_path in storage.items():
            if "s3" in storage_type:
                raise ValueError("Renaming sources that have been uploaded to s3 is not possible.")
            rel_path = rel_path["relativePath"]
            _replace_name_in_data(storage_type, os.path.join(dataset_folder, rel_path), new_name)

    sources[new_name] = {source_type: source_metadata}
    dataset_metadata["sources"] = sources

    # go through all views and rename the source there
    views = dataset_metadata["views"]
    for view_name, view in views.items():
        view = _replace_name_in_view(view, old_name, new_name)
        views[view_name] = view

    # rename the default view for this source (if it exists)
    view = views.pop(old_name, None)
    if view is not None:
        views[new_name] = view

    dataset_metadata["views"] = views

    write_dataset_metadata(dataset_folder, dataset_metadata)
