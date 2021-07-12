

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


def get_affine_source_transform(sources, parameters, timepoints=None):
    assert len(parameters) == 12
    assert all(isinstance(param, float) for param in parameters)
    trafo = {
        "sources": sources,
        "parameters": parameters
    }
    if timepoints is not None:
        trafo["timepoints"] = timepoints
    return {"affine": trafo}


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


def get_view(names, source_types, sources, display_settings,
             is_exclusive, menu_name,
             source_transforms=None, viewer_transform=None, source_annotation_displays=None):
    """ Create view metadata for multi source views.

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
            display = get_image_display(name, source_list, **display_setting)
        elif source_type == "segmentation":
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
            if trafo_type == 'grid':
                assert isinstance(trafo_sources, dict)
                unique_trafo_sources = set([source for grid_source in trafo_sources.values() for source in grid_source])
            else:
                assert isinstance(trafo_sources, list)
                unique_trafo_sources = set(trafo_sources)
            invalid_sources = list(unique_trafo_sources - all_sources)
            if invalid_sources:
                msg = f"Invalid sources in transform: {invalid_sources}"
                raise ValueError(msg)

        view["sourceTransforms"] = source_transforms

    if viewer_transform is not None:
        viewer_transform_types = ['affine', 'normalizedAffine', 'position', 'timepoint']
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
