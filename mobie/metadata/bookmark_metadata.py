import os
import warnings
from copy import deepcopy

from .dataset_metadata import add_view_to_dataset, read_dataset_metadata, write_dataset_metadata
from .utils import read_metadata, write_metadata
from .source_metadata import _get_table_metadata
from .view_metadata import get_view
from ..validation.utils import validate_with_schema
from ..tables.grid_view_table import check_grid_view_table, compute_grid_view_table

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


def _check_bookmark(bookmark_name, bookmarks, overwrite):
    if bookmark_name in bookmarks:
        msg = f"Bookmark {bookmark_name} is already present."
        if overwrite:
            warnings.warn(msg)
        else:
            raise ValueError(msg)


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
    bookmark_file = os.path.join(dataset_folder, "misc", "views", bookmark_file_name)

    metadata = read_metadata(bookmark_file)
    bookmarks = metadata.get("views", {})
    _check_bookmark(bookmark_name, bookmarks, overwrite)

    all_sources = read_dataset_metadata(dataset_folder)['sources']
    view = create_bookmark_view(sources, all_sources, display_settings,
                                source_transforms, viewer_transform,
                                display_group_names)
    validate_with_schema(view, 'view')

    bookmarks[bookmark_name] = view
    metadata['views'] = bookmarks
    write_metadata(bookmark_file, metadata)


def add_grid_bookmark(dataset_folder, name, sources, table_folder=None,
                      display_groups=None, display_group_settings=None,
                      positions=None, bookmark_file_name=None,
                      overwrite=False):
    """ Add or update a grid view.

    Arguments:
        dataset_folder [str] - path to the dataset folder
        name [str] - name of this bookmark
        sources [list[list[str]]] - sources to be arranged in the grid
        table_folder [str] - path to the table folder, relative to the dataset folder (default: None)
        display_groups [dict[str, str] - (default: None)
        display_group_settings [dict[str, dict]] - (default: None)
        positions [list[list[int]]] - (default: None)
        bookmark_file_name [str] - name of the bookmark file,
            will be added to 'views' in datasets.json by default (default: None)
        overwrite [bool] - whether to overwrite existing bookmarks (default: False)
    """
    dataset_metadata = read_dataset_metadata(dataset_folder)
    views = dataset_metadata['views']

    if bookmark_file_name is None:  # bookmark goes into dataset.json:bookmarks
        bookmarks = views
    else:  # bookmark goes into extra bookmark file
        if not bookmark_file_name.endswith('.json'):
            bookmark_file_name += '.json'
        bookmark_file = os.path.join(dataset_folder, "misc", "bookmarks", bookmark_file_name)
        bookmarks = read_metadata(bookmark_file).get('bookmarks', {})
    _check_bookmark(name, bookmarks, overwrite)

    all_sources = dataset_metadata['sources']

    display_names = []
    source_types = []
    display_sources = []
    display_settings = []

    for source_position in sources:
        assert isinstance(source_position, (list, tuple))
        for source_name in source_position:
            if source_name not in all_sources:
                raise ValueError(f"Invalid source name: {source_name}")

            source_type = list(all_sources[source_name].keys())[0]

            if display_groups is None:
                display_name = f'{name}_{source_type}s'
            else:
                display_name = display_groups[source_name]

            if display_name in display_names:
                display_id = display_names.index(display_name)
                display_sources[display_id].append(source_name)
            else:
                display_names.append(display_name)
                source_types.append(source_type)
                display_sources.append([source_name])

                # check if we have display setting parameters
                if display_group_settings is None:
                    # if not, we just take the first source's display settings here
                    display_setting = deepcopy(views[source_name])
                    setting_key = 'imageDisplay' if source_type == 'image' else 'segmentationDisplay'
                    display_setting = display_setting['sourceDisplays'][0][setting_key]
                    display_setting.pop('name')
                    display_setting.pop('sources')
                else:
                    display_setting = display_group_settings[display_name]
                display_settings.append(display_setting)

    # the sources for the grid and the source annotation display need to be dicts
    # with (flat) grid positions mapping to the source names
    map_sources = {ii: sources_pos for ii, sources_pos in enumerate(sources)}

    # create the grid view transform
    grid_transform = {
        "sources": map_sources
    }
    if positions is not None:
        if len(positions) != len(sources):
            msg = f"Invalid grid position length {len(positions)}, expected same length as sources: {len(sources)}"
            raise ValueError(msg)
        # also needs to be a map internally
        grid_transform['positions'] = {ii: pos for ii, pos in enumerate(positions)}
    grid_transform = [{'grid': grid_transform}]

    # process the table folder
    if table_folder is None:
        table_folder = os.path.join('tables', name)
    table_folder_path = os.path.join(dataset_folder, table_folder)
    os.makedirs(table_folder_path, exist_ok=True)
    default_table_path = os.path.join(table_folder_path, 'default.tsv')
    if not os.path.exists(default_table_path):
        compute_grid_view_table(sources, default_table_path, positions=positions)
    check_grid_view_table(sources, default_table_path, positions=positions)

    # create the source annotation display for this grid view, this will show the table for this grid view!
    source_annotation_display = {
        "sources": map_sources,
        "tableData": _get_table_metadata(table_folder),
        "tables": ["default.tsv"]
    }

    view = get_view(names=display_names,
                    source_types=source_types,
                    sources=display_sources,
                    display_settings=display_settings,
                    source_transforms=grid_transform,
                    source_annotation_displays={name: source_annotation_display},
                    is_exclusive=True,
                    menu_name='bookmark')

    validate_with_schema(view, 'view')
    bookmarks[name] = view
    if bookmark_file_name is None:
        dataset_metadata['views'] = bookmarks
        write_dataset_metadata(dataset_folder, dataset_metadata)
    else:
        write_metadata(bookmark_file_name, bookmarks)
