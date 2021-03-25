import os
import warnings

from .dataset_metadata import add_view_to_dataset, read_dataset_metadata
from .utils import read_metadata, write_metadata
from .view_metadata import get_view
from ..validation.utils import validate_with_schema

# TODO add more convenience for source and viewer transforms ?


def create_bookmark_view(sources, all_sources, display_settings,
                         source_transforms, viewer_transform, display_group_names):
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
        display_group_names = [f'{source_type}-group-{i}' for i, source_type in enumerate(source_types)]

    menu_name = "bookmark"
    view = get_view(display_group_names, source_types,
                    sources, display_settings,
                    is_exclusive=True,
                    menu_name=menu_name,
                    source_transforms=source_transforms,
                    viewer_transform=viewer_transform)
    return view


def add_dataset_bookmark(dataset_folder, bookmark_name,
                         sources, display_settings,
                         source_transforms=None, viewer_transform=None,
                         display_group_names=None, overwrite=False):
    """ Add or update a view in dataset.json:views.

    Views can reproduce any given viewer state.

    Arguments:
        dataset_folder [str] - path to the dataset folder
        bookmark_name [str] - name of the view
        sources [list[list[str]]] -
        display_settings [list[dict]] -
        source_transforms [list[dict]] -
        viewer_transform [dict] -
        display_group_names [list[str]] -
        overwrite [bool] - whether to overwrite existing views (default: False)
    """
    all_sources = read_dataset_metadata(dataset_folder)['sources']
    view = create_bookmark_view(sources, all_sources, display_settings,
                                source_transforms, viewer_transform,
                                display_group_names)
    validate_with_schema(view, 'view')
    add_view_to_dataset(dataset_folder, bookmark_name, view, overwrite=overwrite)


def add_additional_bookmark(dataset_folder, bookmark_file_name, bookmark_name,
                            sources, display_settings,
                            source_transforms=None, viewer_transform=None,
                            display_group_names=None, overwrite=False):
    """ Add or update a view in a bookmark file in <dataset_folder>/misc/bookmarks

    Views can reproduce any given viewer state.

    Arguments:
        dataset_folder [str] - path to the dataset folder
        bookmark_file_name [str] - name of the bookmark file
        bookmark_name [str] - name of the bookmark
        overwrite [bool] - whether to overwrite existing bookmarks (default: False)
    """
    if not bookmark_file_name.endswith('.json'):
        bookmark_file_name += '.json'
    bookmark_file = os.path.join(dataset_folder, "misc", "bookmarks", bookmark_file_name)

    metadata = read_metadata(bookmark_file)
    bookmarks = metadata.get("bookmarks", {})

    if bookmark_name in bookmarks:
        msg = f"Bookmark {bookmark_name} is already present in {bookmark_file}"
        if overwrite:
            warnings.warn(msg)
        else:
            raise ValueError(msg)

    all_sources = read_dataset_metadata(dataset_folder)['sources']
    view = create_bookmark_view(sources, all_sources, display_settings,
                                source_transforms, viewer_transform,
                                display_group_names)
    validate_with_schema(view, 'view')

    bookmarks[bookmark_name] = view
    metadata['bookmarks'] = bookmarks
    write_metadata(bookmark_file, metadata)
