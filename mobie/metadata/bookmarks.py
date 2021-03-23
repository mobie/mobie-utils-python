import os
import warnings

from .dataset_metadata import read_dataset_metadata, write_dataset_metadata
from .utils import read_metadata, write_metadata
from .view_metadata import get_view
from ..validation.utils import validate_with_schema


def add_view_to_dataset(dataset_folder, view_name, overwrite=False):
    """ Add or update a view in dataset.json:views.

    Views can reproduce any given viewer state.

    Arguments:
        dataset_folder [str] - path to the dataset folder
        view_name [str] - name of the view
        overwrite [bool] - whether to overwrite existing views (default: False)
    """

    ds_metadata = read_dataset_metadata(dataset_folder)
    views = ds_metadata['views']

    if view_name in views:
        msg = f"View {view_name} is already present in {os.path.join(dataset_folder, 'datasets.json')}"
        if overwrite:
            warnings.warn(msg)
        else:
            raise ValueError(msg)

    view = get_view()
    validate_with_schema(view, 'view')

    views[view_name] = view
    ds_metadata['views'] = views
    write_dataset_metadata(dataset_folder, ds_metadata)


def add_bookmark(dataset_folder, bookmark_file_name, bookmark_name,
                 overwrite=False):
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
    bookmarks = metadata["bookmarks"]

    if bookmark_name in bookmarks:
        msg = f"Bookmark {bookmark_name} is already present in {bookmark_file}"
        if overwrite:
            warnings.warn(msg)
        else:
            raise ValueError(msg)

    view = get_view()
    validate_with_schema(view, 'view')

    bookmarks[bookmark_name] = view
    metadata['bookmarks'] = bookmarks
    write_metadata(bookmark_file, metadata)
