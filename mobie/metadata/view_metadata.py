import os
import warnings
from copy import deepcopy

import numpy as np
from .dataset_metadata import read_dataset_metadata, write_dataset_metadata
from .utils import get_table_metadata
from ..tables import check_region_table, compute_region_table


#
# display settigs
#


def _validate_lut(lut, kwargs):
    numeric_luts = ("viridis", "blueWhiteRed")
    if lut in numeric_luts and "valueLimits" not in kwargs:
        msg = f"You have specified a numeric lut: {lut}. In this case you also need to pass the 'valueLimits' argument."
        raise ValueError(msg)


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
    additional_image_kwargs = ["blendingMode", "resolution3dView", "showImagesIn3d", "visible"]
    for kwarg_name in additional_image_kwargs:
        kwarg_val = kwargs.pop(kwarg_name, None)
        if kwarg_val is not None:
            image_display[kwarg_name] = kwarg_val
    if kwargs:
        raise ValueError(f"Invalid keyword arguments for image display: {list(kwargs.keys())}")
    return {"imageDisplay": image_display}


def get_region_display(name, sources, table_source, **kwargs):
    opacity = kwargs.pop("opacity", 0.5)
    lut = kwargs.pop("lut", "glasbey")
    _validate_lut(lut, kwargs)
    annotation_display = {
        "opacity": opacity,
        "lut": lut,
        "name": name,
        "sources": sources,
        "tableSource": table_source,
    }
    additional_annotation_kwargs = ["additionalTables"
                                    "boundaryThickness",
                                    "boundaryThicknessIsRelative",
                                    "colorByColumn",
                                    "randomColorSeed",
                                    "selectedRegionIds",
                                    "showAsBoundaries",
                                    "showTable",
                                    "valueLimits",
                                    "visible",
                                    "opacityNotSelected",
                                    "selectionColor"]
    for kwarg_name in additional_annotation_kwargs:
        kwarg_val = kwargs.pop(kwarg_name, None)
        if kwarg_val is not None:
            annotation_display[kwarg_name] = kwarg_val
    if kwargs:
        raise ValueError(f"Invalid keyword arguments for source annotation display: {list(kwargs.keys())}")
    return {"regionDisplay": annotation_display}


def get_segmentation_display(name, sources, **kwargs):
    if not isinstance(sources, (list, tuple)) and not all(isinstance(source, str) for source in sources):
        raise ValueError(f"Invalid sources: {sources}")
    opacity = kwargs.pop("opacity", 0.5)
    lut = kwargs.pop("lut", "glasbey")
    _validate_lut(lut, kwargs)
    segmentation_display = {
        "opacity": opacity,
        "lut": lut,
        "name": name,
        "sources": sources
    }
    additional_seg_kwargs = ["boundaryThickness", "colorByColumn",
                             "randomColorSeed", "resolution3dView",
                             "selectedSegmentIds", "showAsBoundaries",
                             "showSelectedSegmentsIn3d", "showTable",
                             "additionalTables", "valueLimits", "visible",
                             "opacityNotSelected", "selectionColor"]
    for kwarg_name in additional_seg_kwargs:
        kwarg_val = kwargs.pop(kwarg_name, None)
        if kwarg_val is not None:
            segmentation_display[kwarg_name] = kwarg_val
    if kwargs:
        raise ValueError(f"Invalid keyword arguments for segmentation display: {list(kwargs.keys())}")
    return {"segmentationDisplay": segmentation_display}


def get_spot_display(name, sources, **kwargs):
    if not isinstance(sources, (list, tuple)) and not all(isinstance(source, str) for source in sources):
        raise ValueError(f"Invalid sources: {sources}")
    opacity = kwargs.pop("opacity", 0.5)
    lut = kwargs.pop("lut", "glasbey")
    _validate_lut(lut, kwargs)
    spot_display = {
        "opacity": opacity,
        "lut": lut,
        "name": name,
        "sources": sources
    }
    additional_seg_kwargs = ["additionalTables",
                             "boundaryThickness", "colorByColumn",
                             "randomColorSeed", "spotRadius",
                             "selectedSpotIds", "showAsBoundaries",
                             "showTable", "valueLimits", "visible",
                             "opacityNotSelected", "selectionColor"]
    for kwarg_name in additional_seg_kwargs:
        kwarg_val = kwargs.pop(kwarg_name, None)
        if kwarg_val is not None:
            spot_display[kwarg_name] = kwarg_val
    if kwargs:
        raise ValueError(f"Invalid keyword arguments for region display: {list(kwargs.keys())}")
    return {"spotDisplay": spot_display}


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
                                timepoints=None, name=None, source_names_after_transform=None):
    assert len(parameters) == 12
    assert all(isinstance(param, float) for param in parameters)
    trafo = {
        "sources": sources,
        "parameters": _ensure_list(parameters)
    }
    if timepoints is not None:
        trafo["timepoints"] = timepoints
    if name is not None:
        trafo["name"] = name
    if source_names_after_transform is not None:
        assert len(source_names_after_transform) == len(sources), f"{source_names_after_transform}, {sources}"
        trafo["sourceNamesAfterTransform"] = source_names_after_transform
    return {"affine": trafo}


def get_crop_source_transform(sources, min, max,
                              timepoints=None, name=None, source_names_after_transform=None,
                              center_at_origin=None, box_affine=None, rectify=None):
    assert len(min) == len(max) == 3
    trafo = {
        "sources": sources,
        "min": _ensure_list(min),
        "max": _ensure_list(max)
    }
    if timepoints is not None:
        trafo["timepoints"] = timepoints
    if name is not None:
        trafo["name"] = name
    if source_names_after_transform is not None:
        assert len(source_names_after_transform) == len(sources)
        trafo["sourceNamesAfterTransform"] = source_names_after_transform
    if center_at_origin is not None:
        trafo["centerAtOrigin"] = center_at_origin
    if box_affine is not None:
        trafo["boxAffine"] = box_affine
    if rectify is not None:
        trafo["rectify"] = rectify
    return {"crop": trafo}


def get_transformed_grid_source_transform(sources, positions=None, source_names_after_transform=None,
                                          timepoints=None, name=None, center_at_origin=None, margin=None):
    # the sources for the grid trafo need to be dicts. if a list is given, we just use the indices as keys
    assert isinstance(sources, list)
    assert all(isinstance(source_pos, list) for source_pos in sources)

    grid_transform = {"nestedSources": sources}

    if positions is not None:
        assert isinstance(positions, list)
        msg = f"Invalid grid position length {len(positions)}, expected same length as sources: {len(sources)}"
        assert len(positions) == len(sources), msg
        grid_transform["positions"] = positions

    if source_names_after_transform is not None:
        assert isinstance(source_names_after_transform, list)
        assert all(isinstance(source_pos, list) for source_pos in source_names_after_transform)
        assert len(source_names_after_transform) == len(sources)
        grid_transform["sourceNamesAfterTransform"] = source_names_after_transform

    if timepoints is not None:
        grid_transform["timepoints"] = timepoints
    if name is not None:
        grid_transform["name"] = name

    if center_at_origin is not None:
        grid_transform["centerAtOrigin"] = center_at_origin

    if margin is not None:
        grid_transform["margin"] = margin

    return {"transformedGrid": grid_transform}


def get_merged_grid_source_transform(sources, merged_source_name,
                                     positions=None, timepoints=None,
                                     name=None, center_at_origin=None,
                                     metadata_source=None, margin=None):
    assert isinstance(sources, list)
    grid_transform = {"sources": sources, "mergedGridSourceName": merged_source_name}

    if positions is not None:
        assert len(positions) == len(sources)
        grid_transform["positions"] = positions

    if timepoints is not None:
        grid_transform["timepoints"] = timepoints

    if center_at_origin is not None:
        warnings.warn("Passing centerAtOrigin does not have any effect for the mergedGrid")

    if metadata_source is not None:
        grid_transform["metadataSource"] = metadata_source

    if margin is not None:
        grid_transform["margin"] = margin

    return {"mergedGrid": grid_transform}


#
# viewer transformation
#


def get_viewer_transform(affine=None, normalized_affine=None, position=None, normal_vector=None, timepoint=None):
    # don't allow empty transform
    if all(param is None for param in (affine, normalized_affine, position, normal_vector, timepoint)):
        raise ValueError("Invalid parameters: need to pass at least one parameter")

    trafo = {}
    if affine is not None:
        if normalized_affine is not None:
            raise ValueError("Invalid parameters: both affine and normalized_affine were passed")
        if position is not None:
            raise ValueError("Invalid parameters: both affine and position were passed")
        if normal_vector is not None:
            raise ValueError("Invalid parameters: both affine and normal_vector were passed")
        assert len(affine) == 12
        assert all(isinstance(param, float) for param in affine)
        trafo["affine"] = affine

    if normalized_affine is not None:
        if position is not None:
            raise ValueError("Invalid parameters: both normalized_affine and position were passed")
        if normal_vector is not None:
            raise ValueError("Invalid parameters: both normalized_affine and normal_vector were passed")
        assert len(normalized_affine) == 12
        assert all(isinstance(param, float) for param in normalized_affine)
        trafo["normalizedAffine"] = normalized_affine

    if position is not None:
        if normal_vector is not None:
            raise ValueError("Invalid parameters: both position and normal_vector were passed")
        assert len(position) == 3
        assert all(isinstance(param, float) for param in position)
        trafo["position"] = position

    if normal_vector is not None:
        assert len(normal_vector) == 3
        assert all(isinstance(param, float) for param in normal_vector)
        trafo["normalVector"] = normal_vector

    if timepoint is not None:
        trafo["timepoint"] = timepoint

    return trafo


#
# view functionality
#


def get_view(names, source_types, sources, display_settings,
             is_exclusive, menu_name, description=None,
             source_transforms=None, viewer_transform=None, region_displays=None):
    """ Create view for a multiple sources and optional transformations.

    Arguments:
        names [list[str]] - names of the display groups in this view.
        source_types [list[str]] - list of source types in this view.
        sources [list[list[str]]] - nested list of source names in this view.
        display_settings [list[dict]] - list of display settings in this view.
        is_exclusive [bool] - is this an exclusive view.
        menu_name [str] - menu name for this view
        description [str] - description for this view (default: None)
        source_transforms [list[dict]] - (default: None)
        viewer_transform [dict] - (default: None)
        region_displays dict[str, dict] - dictionary from region display name
            to the region display settings (default: None)
    """

    if not len(names) == len(source_types) == len(sources) == len(display_settings):
        lens = f"{len(names)} {len(source_types)}, {len(sources)}, {len(display_settings)}"
        raise ValueError(f"Different length of names, types, sources and settings: {lens}")
    view = {"isExclusive": is_exclusive, "uiSelectionGroup": menu_name}
    if description is not None:
        view["description"] = description

    source_displays = []
    for name, source_type, source_list, display_setting in zip(names, source_types, sources, display_settings):

        if source_type == "image":
            # display settings can either be passed as arguments or return values of get_image_display
            if "imageDisplay" in display_setting:
                assert len(display_setting) == 1
                assert display_setting["imageDisplay"]["name"] == name,\
                    f"{display_setting['imageDisplay']['name']}, {name}"
                _sources = display_setting["imageDisplay"]["sources"]
                invalid_sources = set(_sources) - set(source_list)
                assert len(invalid_sources) == 0,\
                    f"The settings for {name} contain invalid sources: {invalid_sources} not in {source_list}"
                display = display_setting
            else:
                this_display_sources = display_setting.pop("sources", source_list)
                display = get_image_display(name, this_display_sources, **display_setting)

        elif source_type == "segmentation":
            # display settings can either be passed as arguments or return values of get_segmentation_display
            if "segmentationDisplay" in display_setting:
                assert len(display_setting) == 1
                assert display_setting["segmentationDisplay"]["name"] == name,\
                    f"{display_setting['segmentationDisplay']['name']}, {name}"
                _sources = display_setting["segmentationDisplay"]["sources"]
                invalid_sources = set(_sources) - set(source_list)
                assert len(invalid_sources) == 0,\
                    f"The settings for {name} contain invalid sources: {invalid_sources} not in {source_list}"
                display = display_setting
            else:
                display = get_segmentation_display(name, source_list, **display_setting)

        elif source_type == "spots":
            # display settings can either be passed as arguments or return values of get_spot_display
            if "spotDisplay" in display_settings:
                assert len(display_setting) == 1
                assert display_setting["spotDisplay"]["name"] == name,\
                    f"{display_setting['spotDisplay']['name']}, {name}"
                _sources = display_setting["spotDisplay"]["sources"]
                invalid_sources = set(_sources) - set(source_list)
                assert len(invalid_sources) == 0,\
                    f"The settings for {name} contain invalid sources: {invalid_sources} not in {source_list}"
                display = display_setting
            else:
                display = get_spot_display(name, source_list, **display_setting)

        else:
            raise ValueError(f"Invalid source_type {source_type}, expect one of 'image', 'segmentation' or 'spots'")

        source_displays.append(display)

    if region_displays is not None:
        for name, settings in region_displays.items():
            source_map = settings.pop("sources")
            table_source = settings.pop("tableSource")
            assert isinstance(source_map, dict)
            display = get_region_display(name, source_map, table_source, **settings)
            source_displays.append(display)

    view["sourceDisplays"] = source_displays

    if source_transforms is not None:
        valid_source_transforms = {"affine", "crop", "mergedGrid", "transformedGrid"}
        this_source_transforms = set([list(trafo.keys())[0] for trafo in source_transforms])
        invalid_trafos = list(this_source_transforms - valid_source_transforms)
        if invalid_trafos:
            msg = f"Invalid source transforms: {invalid_trafos}, only {valid_source_transforms} are valid"
            raise ValueError(msg)
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
                     source_transform=None, viewer_transform=None,
                     description=None, **kwargs):
    """ Create default view metadata for a single source.

    Arguments:
        source_type [str] - type of the source, either "image", "segmentation" or "spots"
        source_name [str] - name of the source.
        menu_name [str] - menu name for this view (default: None)
        source_transform [dict] - dict with affine source transform.
            If given, must contain "parameters" and may contain "timepoints" (default: None).
        viewer_transform [dict] - dict with viewer transform (default: None)
        description [str] - description for this view (default: None).
        **kwargs - additional settings for this view
    """
    if menu_name is None:
        menu_name = source_type if source_type.endswith("s") else f"{source_type}s"
    if source_transform is None:
        source_transforms = None
    else:
        source_transforms = [
            get_affine_source_transform(
                [source_name], source_transform["parameters"], source_transform.get("timepoints", None)
            )
        ]

    view = get_view([source_name], [source_type], [[source_name]], [kwargs],
                    is_exclusive=False, menu_name=menu_name, description=description,
                    source_transforms=source_transforms, viewer_transform=viewer_transform)
    return view


def _to_transformed_grid(sources, positions, center_at_origin):
    grid_trafo = get_transformed_grid_source_transform(sources, positions, center_at_origin=center_at_origin)
    return [grid_trafo]


def _to_merged_grid(sources, name, positions, center_at_origin):
    assert isinstance(sources, (dict, list))
    grid_sources = sources if isinstance(sources, list) else list(sources.values())
    sources_per_pos = len(grid_sources[0])
    assert all(len(sor) == sources_per_pos for sor in grid_sources)
    source_transforms = [
        get_merged_grid_source_transform(
           [source[ii] for source in grid_sources], f"{name}-{ii}",
           positions=positions, center_at_origin=center_at_origin,
        ) for ii in range(sources_per_pos)
    ]
    return source_transforms


def require_region_table(dataset_folder, table_source, table_folder, this_sources):
    ds_metadata = read_dataset_metadata(dataset_folder)
    sources = ds_metadata["sources"]

    if table_source in sources:
        table_folder_path = os.path.join(
            dataset_folder, sources[table_source]["regions"]["tableData"]["tsv"]["relativePath"]
        )
        default_table_path = os.path.join(table_folder_path, "default.tsv")
        check_region_table(this_sources, default_table_path)
    else:
        # create and write the default region table
        table_folder_path = os.path.join(dataset_folder, table_folder)
        os.makedirs(table_folder_path, exist_ok=True)
        default_table_path = os.path.join(table_folder_path, "default.tsv")
        compute_region_table(this_sources, default_table_path)
        check_region_table(this_sources, default_table_path)

        # create the table source
        table_data = get_table_metadata(table_folder)
        # this should eventually be wrapped in a function,
        # in case we get more properties for the region table source
        sources[table_source] = {"regions": table_data}
        ds_metadata["sources"] = sources
        write_dataset_metadata(dataset_folder, ds_metadata)


def create_region_display(name, sources, dataset_folder, table_source, table_folder=None, region_ids=None, **kwargs):
    """Get a region display and create the corresponding table.
    """
    if isinstance(sources, list) and region_ids is None:
        sources = {ii: source_list for ii, source_list in enumerate(sources)}
    elif isinstance(sources, list):
        assert len(sources) == len(region_ids)
        sources = {region_id: source_list for region_id, source_list in zip(region_ids, sources)}
    assert isinstance(sources, dict)
    assert all(isinstance(source_list, list) for source_list in sources.values())

    require_region_table(dataset_folder, table_source,
                         table_folder=os.path.join("tables", name) if table_folder is None else table_folder,
                         this_sources=sources)
    region_display = get_region_display(name, sources, table_source, **kwargs)["regionDisplay"]
    region_display.pop("name")

    return {name: region_display}


# supporting grid views with transform (if trafo names change) is currently rather cumbersome:
# "grid_sources" need to be passed as dict and specify the correct names
# (i.e. names after transform). dict needs to match from the grid id
# to list of source names
def get_grid_view(dataset_folder, name, sources, menu_name,
                  table_source=None, table_folder=None, display_groups=None,
                  display_group_settings=None, positions=None,
                  grid_sources=None, center_at_origin=None,
                  additional_source_transforms=None,
                  use_transformed_grid=True, region_ids=None):
    """ Create a view that places multiple sources in a grid.

    Arguments:
        dataset_folder [str] - the folder for this dataset
        name [str] - name of this view
        sources [list[list[str]]] - nested list of source names,
            each inner lists contains the source(s) for one grid position
        menu_name [str] - menu name for this view
        table_source [str] - name of the table source for the region display that is created
            for this grid view. If the source is not present yet it will be created.
            If the table source is None than no region table and display will be created for this view (default: None)
        table_folder [str] - table folder to store the annotation table(s) for this grid.
            By default "tables/{name}" will be used (default: None)
        display_groups [dict[str, str]] - dictionary from source name to their display group.
            By default each source type is put into the same display group (default: None)
        display_group_settings [dict[str, dict]] - dictionary from display group name to settings.
            By default the standard settings for the first source of the group are used (default: None)
        positions [list[Sequence[int]]] - cartesian grid position for the grid points.
            By default the grid is auto-created (default: None)
        grid_sources [list[list[str]]] - optional nested list of source names for each grid position.
            Replaces the names in `sources` that are used in the sourceDisplays of the view.
            Passing grid sources can be used to show sources that are generated by an additional
            transform in this view. (default: None)
        center_at_origin [bool] - whether to center the sources at the origin across the z-axis (default: None)
        additional_source_transforms [list[source_transforms]] - list of source transforms to
            be applied before the grid transform. (default: None)
        use_transformed_grid [bool] - Whether to use a transformedGrid, which does not merge all sources
            into a single source in the MoBIE viewer (default: True)
        region_ids [list[str]] - Custom keys for the regionDisplay source map (default: None)
    """
    assert len(sources) > 1, "A grid view needs at least 2 grid positions."

    dataset_metadata = read_dataset_metadata(dataset_folder)
    all_sources = dataset_metadata["sources"]
    views = dataset_metadata["views"]

    display_names = []
    source_types = []
    display_sources = []
    display_settings = []

    # if `grid_sources` are passed, they are written into the displays of the view
    # and replace the names listed in `sources`.
    # this allows specifying "transitive" sources that are produced by a transform in this view
    # the names in sources are always used to fetch the display settings
    if grid_sources is None:
        grid_sources = sources
    else:
        assert len(grid_sources) == len(sources), f"{len(grid_sources)}, {len(sources)}"
        assert all(
            len(source_position) == len(names_for_view)
            for source_position, names_for_view in zip(sources, grid_sources)
        )

    for source_position, names_for_view in zip(sources, grid_sources):
        assert isinstance(source_position, (list, tuple)), f"{type(source_position)}: {source_position}"

        for source_name, name_for_view in zip(source_position, names_for_view):
            if source_name not in all_sources:
                raise ValueError(f"Invalid source name: {source_name}")

            source_type = list(all_sources[source_name].keys())[0]

            if display_groups is None:
                display_name = f"{name}_{source_type}s"
            else:
                display_name = display_groups[source_name]

            if display_name in display_names:
                display_id = display_names.index(display_name)
                display_sources[display_id].append(name_for_view)
            else:
                display_names.append(display_name)
                source_types.append(source_type)
                display_sources.append([name_for_view])

                # check if we have display setting parameters
                if display_group_settings is None:
                    # if not, we just take the first source"s display settings here
                    display_setting = deepcopy(views[source_name])
                    display_setting = next(iter(display_setting["sourceDisplays"][0].values()))
                    display_setting.pop("name")
                    display_setting.pop("sources")
                else:
                    display_setting = display_group_settings[display_name]
                display_settings.append(display_setting)

    # create the grid transform
    if use_transformed_grid:
        source_transforms = _to_transformed_grid(grid_sources, positions, center_at_origin)
    else:
        source_transforms = _to_merged_grid(grid_sources, name, positions, center_at_origin)

    if additional_source_transforms is not None:
        assert isinstance(additional_source_transforms, list)
        source_transforms = additional_source_transforms + source_transforms

    if table_source is None:
        region_displays = None
    else:
        region_displays = create_region_display(name, grid_sources, dataset_folder,
                                                table_source=table_source, table_folder=table_folder,
                                                region_ids=region_ids)
    view = get_view(names=display_names,
                    source_types=source_types,
                    sources=display_sources,
                    display_settings=display_settings,
                    source_transforms=source_transforms,
                    region_displays=region_displays,
                    is_exclusive=True,
                    menu_name=menu_name)
    return view


def is_grid_view(view):
    trafos = view.get("sourceTransforms", None)
    if trafos is None:
        return False
    for trafo in trafos:
        if list(trafo.keys())[0] == "grid":
            return True
    return False
