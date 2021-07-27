import os
from copy import deepcopy

import numpy as np
from .dataset_metadata import read_dataset_metadata
from .utils import get_table_metadata
from ..tables.grid_view_table import check_grid_view_table, compute_grid_view_table


#
# display settigs
#


def get_image_display(name, sources, **kwargs):
    if not isinstance(sources, (list, tuple)) and not all(isinstance(source, str) for source in sources):
        raise ValueError(f"Invalid sources: {sources}")
    color = kwargs.pop("color", "white")
    contrast_limits = kwargs.pop("contrastLimits",  [0.0, 255.0])
    opacity = kwargs.pop("opacity", 1.)
    image_display = {
        "color": color,
        "contrastLimits": contrast_limits,
        "name": name,
        "opacity": opacity,
        "sources": sources
    }
    additional_image_kwargs = ["blendingMode", "resolution3dView", "showImagesIn3d"]
    for kwarg_name in additional_image_kwargs:
        kwarg_val = kwargs.pop(kwarg_name, None)
        if kwarg_val is not None:
            image_display[kwarg_name] = kwarg_val
    if kwargs:
        raise ValueError(f"Invalid keyword arguments for image display: {list(kwargs.keys())}")
    return {"imageDisplay": image_display}


def get_segmentation_display(name, sources, **kwargs):
    if not isinstance(sources, (list, tuple)) and not all(isinstance(source, str) for source in sources):
        raise ValueError(f"Invalid sources: {sources}")
    opacity = kwargs.pop("opacity", 0.5)
    lut = kwargs.pop("lut", "glasbey")
    segmentation_display = {
        "opacity": opacity,
        "lut": lut,
        "name": name,
        "sources": sources
    }
    additional_seg_kwargs = ["blendingMode", "colorByColumn", "resolution3dView",
                             "selectedSegmentIds", "showSelectedSegmentsIn3d",
                             "tables", "valueLimits"]
    for kwarg_name in additional_seg_kwargs:
        kwarg_val = kwargs.pop(kwarg_name, None)
        if kwarg_val is not None:
            segmentation_display[kwarg_name] = kwarg_val
    if kwargs:
        raise ValueError(f"Invalid keyword arguments for segmentation display: {list(kwargs.keys())}")
    return {"segmentationDisplay": segmentation_display}


def get_source_annotation_display(name, sources, table_data, tables, **kwargs):
    opacity = kwargs.pop("opacity", 0.5)
    lut = kwargs.pop("lut", "glasbey")
    annotation_display = {
        "opacity": opacity,
        "lut": lut,
        "name": name,
        "sources": sources,
        "tableData": table_data,
        "tables": tables
    }
    additional_annotation_kwargs = ["colorByColumn",
                                    "selectedAnnotationIds",
                                    "valueLimits"]
    for kwarg_name in additional_annotation_kwargs:
        kwarg_val = kwargs.pop(kwarg_name, None)
        if kwarg_val is not None:
            annotation_display[kwarg_name] = kwarg_val
    if kwargs:
        raise ValueError(f"Invalid keyword arguments for source annotation display: {list(kwargs.keys())}")
    return {"sourceAnnotationDisplay": annotation_display}


#
# source transformations
#

def _ensure_list(x):
    if isinstance(x, tuple):
        return list(x)
    if isinstance(x, np.ndarray):
        return x.tolist()
    assert isinstance(x, list)
    return x


def get_affine_source_transform(sources, parameters,
                                timepoints=None, source_names_after_transform=None):
    assert len(parameters) == 12
    assert all(isinstance(param, float) for param in parameters)
    trafo = {
        "sources": sources,
        "parameters": _ensure_list(parameters)
    }
    if timepoints is not None:
        trafo["timepoints"] = timepoints
    if source_names_after_transform is not None:
        assert len(source_names_after_transform) == len(sources), f"{source_names_after_transform}, {sources}"
        trafo["sourceNamesAfterTransform"] = source_names_after_transform
    return {"affine": trafo}


def get_crop_source_transform(sources, min, max,
                              timepoints=None, source_names_after_transform=None,
                              shift_to_origin=None):
    assert len(min) == len(max) == 3
    trafo = {
        "sources": sources,
        "min": _ensure_list(min),
        "max": _ensure_list(max)
    }
    if timepoints is not None:
        trafo["timepoints"] = timepoints
    if source_names_after_transform is not None:
        assert len(source_names_after_transform) == len(sources)
        trafo["sourceNamesAfterTransform"] = source_names_after_transform
    if shift_to_origin is not None:
        trafo["shiftToOrigin"] = shift_to_origin
    return {"crop": trafo}


def get_grid_source_transform(sources, positions=None, source_names_after_transform=None, timepoints=None):
    # the sources for the grid trafo need to be dicts. if a list is given, we just use the indices as keys
    if isinstance(sources, list):
        sources = {ii: sources_pos for ii, sources_pos in enumerate(sources)}
    assert isinstance(sources, dict)

    grid_transform = {"sources": sources}

    if positions is not None:
        msg = f"Invalid grid position length {len(positions)}, expected same length as sources: {len(sources)}"
        assert len(positions) == len(sources), msg
        if isinstance(positions, list):
            positions = {k: pos for k, pos in zip(sources.keys(), positions)}
        assert len(set(positions.keys()) - set(sources.keys())) == 0
        grid_transform["positions"] = positions

    if source_names_after_transform is not None:
        assert len(source_names_after_transform) == len(sources)
        if isinstance(source_names_after_transform, list):
            source_names_after_transform = {k: name for k, name in zip(sources.keys(), source_names_after_transform)}
        assert len(set(source_names_after_transform.keys()) - set(sources.keys())) == 0
        grid_transform["sourceNamesAfterTransform"] = source_names_after_transform

    if timepoints is not None:
        grid_transform["timepoints"] = timepoints

    return {"grid": grid_transform}


def get_grid_view(dataset_folder, name, sources, menu_name=None,
                  table_folder=None, display_groups=None,
                  display_group_settings=None, positions=None,
                  grid_sources=None,
                  additional_source_transforms=None):
    dataset_metadata = read_dataset_metadata(dataset_folder)
    all_sources = dataset_metadata["sources"]
    views = dataset_metadata["views"]

    display_names = []
    source_types = []
    display_sources = []
    display_settings = []

    for source_position in sources:
        assert isinstance(source_position, (list, tuple)), f"{type(source_position)}: {source_position}"
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

    # create the grid transform
    if grid_sources is None:
        grid_sources = sources
    grid_trafo = get_grid_source_transform(grid_sources, positions)
    grid_sources = grid_trafo["grid"]["sources"]
    if additional_source_transforms is None:
        source_transforms = [grid_trafo]
    else:
        assert isinstance(additional_source_transforms, list)
        source_transforms = additional_source_transforms + [grid_trafo]

    # process the table folder
    if table_folder is None:
        table_folder = os.path.join('tables', name)
    table_folder_path = os.path.join(dataset_folder, table_folder)
    os.makedirs(table_folder_path, exist_ok=True)
    default_table_path = os.path.join(table_folder_path, 'default.tsv')
    if not os.path.exists(default_table_path):
        compute_grid_view_table(grid_sources, default_table_path)
    check_grid_view_table(grid_sources, default_table_path)

    # create the source annotation display for this grid view, this will show the table for this grid view!
    source_annotation_display = {
        "sources": grid_sources,
        "tableData": get_table_metadata(table_folder),
        "tables": ["default.tsv"]
    }

    if menu_name is None:
        menu_name = "grid"
    view = get_view(names=display_names,
                    source_types=source_types,
                    sources=display_sources,
                    display_settings=display_settings,
                    source_transforms=source_transforms,
                    source_annotation_displays={name: source_annotation_display},
                    is_exclusive=True,
                    menu_name=menu_name)
    return view


#
# viewer transformation
#


def get_viewer_transform(affine=None, normalized_affine=None, position=None, timepoint=None):
    # don't allow empty transform
    if all(param is None for param in (affine, normalized_affine, position, timepoint)):
        raise ValueError("Invalid parameters: need to pass at least one parameter")

    trafo = {}
    if affine is not None:
        if normalized_affine is not None:
            raise ValueError("Invalid parameters: both affine and normalized_affine were passed")
        if position is not None:
            raise ValueError("Invalid parameters: both affine and position were passed")
        assert len(affine) == 12
        assert all(isinstance(param, float) for param in affine)
        trafo['affine'] = affine

    if normalized_affine is not None:
        if position is not None:
            raise ValueError("Invalid parameters: both normaized affine and position were passed")
        assert len(normalized_affine) == 12
        assert all(isinstance(param, float) for param in normalized_affine)
        trafo['normalizedAffine'] = normalized_affine

    if position is not None:
        assert len(position) == 3
        assert all(isinstance(param, float) for param in position)
        trafo['position'] = position

    if timepoint is not None:
        trafo['timepoint'] = timepoint

    return trafo


#
# view functionality
#


def get_view(names, source_types, sources, display_settings,
             is_exclusive, menu_name,
             source_transforms=None, viewer_transform=None, source_annotation_displays=None):
    """ Create view for a multiple sources and optional transformations.

    Arguments:
        names [list[str]] - names of the display groups in this view.
        source_types [list[str]] - list of source types in this view.
        sources [list[list[str]]] - nested list of source names in this view.
        display_settings [list[dict]] - list of display settings in this view.
        is_exclusive [bool] - is this an exclusive view.
        menu_name [str] - menu name for this view
        source_transforms [list[dict]] - (default: None)
        viewer_transform [dict] - (default: None)
        source_annotation_displays [list[dict]] - (default: None)
    """

    if len(names) != len(source_types) != len(sources) != len(display_settings):
        lens = f"{len(names)} {len(source_types)}, {len(sources)}, {len(display_settings)}"
        raise ValueError(f"Different length of names, types, sources and settings: {lens}")
    view = {"isExclusive": is_exclusive, "uiSelectionGroup": menu_name}

    source_displays = []
    for name, source_type, source_list, display_setting in zip(names, source_types, sources, display_settings):

        if source_type == "image":
            # display settings can either be passed as arguments or return values of get_image_display
            if "imageDisplay" in display_setting:
                assert len(display_setting) == 1
                assert display_setting["imageDisplay"]["name"] == name
                _sources = display_setting["imageDisplay"]["sources"]
                assert len(set(_sources) - set(source_list)) == 0
                display = display_setting
            else:
                display = get_image_display(name, source_list, **display_setting)

        elif source_type == "segmentation":
            # display settings can either be passed as arguments or return values of get_segmentation_display
            if "segmentationDisplay" in display_setting:
                assert len(display_setting) == 1
                assert display_setting["segmentationDisplay"]["name"] == name
                _sources = display_setting["segmentationDisplay"]["sources"]
                assert len(set(_sources) - set(source_list)) == 0
                display = display_setting
            else:
                display = get_segmentation_display(name, source_list, **display_setting)

        else:
            raise ValueError(f"Invalid source_type {source_type}, expect one of 'image' or 'segmentation'")

        source_displays.append(display)

    if source_annotation_displays is not None:
        for name, settings in source_annotation_displays.items():
            source_map = settings.pop("sources")
            table_data = settings.pop("tableData")
            assert isinstance(source_map, dict)
            display = get_source_annotation_display(name, source_map, table_data, **settings)
            source_displays.append(display)

    view["sourceDisplays"] = source_displays

    if source_transforms is not None:

        # check that source transform types are valid and that all sources listed
        # are also present in the display sources
        all_sources = set([source for source_list in sources for source in source_list])
        for source_transform in source_transforms:
            trafo_type = list(source_transform.keys())[0]
            if trafo_type not in ("affine", "grid", "crop"):
                msg = f"Invalid source transform type {trafo_type}, expect one of 'affine', 'grid', 'crop'"
                raise ValueError(msg)

            trafo = source_transform[trafo_type]
            trafo_sources = trafo["sources"]
            if trafo_type == "grid":
                assert isinstance(trafo_sources, dict)
                unique_trafo_sources = set([source for grid_source in trafo_sources.values() for source in grid_source])
            else:
                assert isinstance(trafo_sources, list)
                unique_trafo_sources = set(trafo_sources)
            invalid_sources = list(unique_trafo_sources - all_sources)
            if invalid_sources:
                msg = f"Invalid sources in transform: {invalid_sources}"
                raise ValueError(msg)

            # we need to add 'sourceNamesAfterTransform' if they are given
            if "sourceNamesAfterTransform" in trafo:
                additional_names = trafo["sourceNamesAfterTransform"]
                if isinstance(additional_names, dict):
                    additional_names = list(additional_names.values())
                all_sources = all_sources.union(set(additional_names))

        view["sourceTransforms"] = source_transforms

    if viewer_transform is not None:
        viewer_transform_types = ["affine", "normalizedAffine", "position", "timepoint"]
        viewer_transform_type = list(viewer_transform.keys())[0]
        if len(viewer_transform) != 1 and viewer_transform_type not in viewer_transform_types:
            msg = f"Invalid viewer transform {viewer_transform_type}, expect one of {viewer_transform_types}"
            raise ValueError(msg)
        view["viewerTransform"] = viewer_transform

    return view


def get_default_view(source_type, source_name, menu_name=None,
                     source_transform=None, viewer_transform=None, **kwargs):
    """ Create default view metadata for a single source.

    Arguments:
        source_type [str] - type of the source, either 'image' or 'segmentation'
        source_name [str] - name of the source.
        menu_name [str] - menu name for this view (default: None)
        source_transform [dict] - dict with affine source transform.
            If given, must contain 'parameters' and may contain 'timepoints' (default: None).
        viewer_transform [dict] - dict with viewer transform (default: None)
        **kwargs - additional settings for this view
    """
    menu_name = f"{source_type}s" if menu_name is None else menu_name
    if source_transform is None:
        source_transforms = None
    else:
        source_transforms = [
            get_affine_source_transform(
                [source_name], source_transform["parameters"], source_transform.get("timepoints", None)
            )
        ]

    view = get_view([source_name], [source_type], [[source_name]], [kwargs],
                    is_exclusive=False, menu_name=menu_name,
                    source_transforms=source_transforms, viewer_transform=viewer_transform)
    return view


def is_grid_view(view):
    trafos = view.get("sourceTransforms", None)
    if trafos is None:
        return False
    for trafo in trafos:
        if list(trafo.keys())[0] == 'grid':
            return True
    return False
