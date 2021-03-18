import os
from .utils import read_metadata, write_metadata


def add_bookmark(
    dataset_folder,
    bookmark_file_name,
    bookmark_name,
    view,
    overwrite=False
):
    """ Add a bookmark entry.

    Bookmarks can set the viewer to any given viewer state.

    Arguments:
        dataset_folder [str] - path to the dataset folder
        bookmark_file_name [str] - name of the bookmark file
        bookmark_name [str] - name of the bookmark
        view [dict] - the viewer state for this bookmark
        overwrite [bool] - whether to overwrite existing bookmarks (default: False)
    """
    bookmark_folder = os.path.join(dataset_folder, 'misc', 'bookmarks')
    bookmark_path = os.path.join(bookmark_folder, f'{bookmark_file_name}.json')

    # TODO validate the view

    bookmarks = read_metadata(bookmark_path)
    if bookmark_name in bookmarks and not overwrite:
        raise ValueError(f"Bookmark {bookmark_name} is already present and overwrite was set to false")
    bookmarks[bookmark_name] = view

    write_metadata(bookmark_path, bookmarks)
