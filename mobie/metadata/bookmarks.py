import json
import os


def validate_bookmark_settings(settings):
    pass


def add_bookmark(dataset_folder, bookmark_file_name,
                 bookmark_name, layer_name, settings,
                 overwrite=False):
    """ Add a bookmark entry.

    Arguments:
        dataset_folder [str] - path to the dataset folder
        bookmark_file_name [str] - name of the bookmark file
        bookmark_name [str] - name of the bookmark entry
        layer_name [str or list[str]] - name of the layer(s)
        settings [dict or list[dict]] - settings for the layer(s)
        overwrite [bool] - whether to overwrite existing entries (default: False)
    """
    bookmark_folder = os.path.join(dataset_folder, 'misc', 'bookmarks')
    os.makedirs(bookmark_folder, exist_ok=True)

    bookmark_path = os.path.join(bookmark_folder, f'{bookmark_file_name}.json')

    if os.path.exists(bookmark_path):
        with open(bookmark_path) as f:
            bookmarks = json.load(f)
    else:
        bookmarks = {}

    if isinstance(layer_name, str):
        validate_bookmark_settings(settings)
        layer_dict = {layer_name: settings}
    else:
        for sett in settings:
            validate_bookmark_settings(sett)
        layer_dict = {name: sett for name, sett in zip(layer_name, settings)}

    bookmark_entry = {
        'layers': layer_dict
    }

    bookmarks[bookmark_name] = bookmark_entry
    with open(bookmark_path, 'w') as f:
        json.dump(bookmarks, f)
