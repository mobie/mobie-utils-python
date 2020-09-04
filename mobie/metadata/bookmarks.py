import json
import os


def validate_bookmark_settings(im_dict, layer_name, settings):
    im_settings = im_dict.get(layer_name, None)
    if im_settings is None:
        raise ValueError(f"Could not find layer {layer_name} in images.json")

    # TODO validate the bookmark settings based on the layer type
    # layer_type = im_dict['type']
    # for setting in settings:
    #     pass


def add_bookmark(dataset_folder, bookmark_file_name, bookmark_name,
                 position=None, view=None,
                 layer_settings=None, overwrite=False):
    """ Add a bookmark entry.

    Bookmarks can navigate the viewer to a given position and view and/or
    add layers and change layer settings.

    Arguments:
        dataset_folder [str] - path to the dataset folder
        bookmark_file_name [str] - name of the bookmark file
        bookmark_name [str] - name of the bookmark entry
        position [listlike] - position (in physical coordinates) for this bookmark (default: None)
        view [listlike] - view transformation for this bookmark,
            expected as bdv affine transformation parameters (default: None)
        layer_settings [dict[dict]] - layers to be added for the bookmark and their settings (default: None)
        overwrite [bool] - whether to overwrite existing entries (default: False)
    """
    bookmark_folder = os.path.join(dataset_folder, 'misc', 'bookmarks')
    bookmark_path = os.path.join(bookmark_folder, f'{bookmark_file_name}.json')

    # validate the arguments
    have_view = view is not None
    if have_view and len(view) != 12:
        raise ValueError(f"Invalid view argument, expect 12 parameters, got {len(view)}")

    have_position = position is not None
    if have_position and len(position) != 3:
        raise ValueError(f"Invalid position argument, expect 3 parameters, got {len(position)}")

    have_layers = layer_settings is not None

    # need to have at least one of view, layer or position
    if sum((have_view, have_position, have_layers)) == 0:
        raise ValueError("Neither position, view nor layer settings were specified")

    # load existing bookmarks if we have them
    if os.path.exists(bookmark_path):
        with open(bookmark_path) as f:
            bookmarks = json.load(f)
    else:
        bookmarks = {}

    if bookmark_name in bookmarks and not overwrite:
        raise ValueError(f"Bookmark {bookmark_name} is already present and overwrite was set to false")

    # make the bookmark entry
    bookmark_entry = {}
    if have_position:
        bookmark_entry['position'] = position

    if have_view:
        bookmark_entry['view'] = view

    if have_layers:

        # load layer names from the image dict to check that the names are valid
        im_dict_path = os.path.join(dataset_folder, 'images', 'images.json')
        with open(im_dict_path) as f:
            im_dict = json.load(f)

        # validate the layer_settings
        for layer_name, settings in layer_settings.items():
            validate_bookmark_settings(im_dict, layer_name, settings)

        bookmark_entry['layers'] = layer_settings

    bookmarks[bookmark_name] = bookmark_entry
    with open(bookmark_path, 'w') as f:
        json.dump(bookmarks, f, indent=2, sort_keys=True)
