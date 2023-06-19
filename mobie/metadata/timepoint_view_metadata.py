from .dataset_metadata import read_dataset_metadata
from .source_metadata import get_timepoints
from .view_metadata import get_image_display, get_region_display, get_segmentation_display, get_view


def get_timepoints_transform(source, dataset_folder, target, sourceidx=None,
                             targetidx=None, keep=False, name=None, source_names_after_transform=None):
    """
    Creates a timepoint transformation mapping timepoints from one source to timepoints of a target source

    Arguments:
        source [str] - name of the source to be mapped
        dataset_folder [str] - the folder for this dataset
        target [str] - name of the target source
        sourceidx [list[int]] - indeces of the source to be mapped
        targetidx [list[int]] - subset of target indeces
        keep [bool] - whether other timepoints of the source are still available
        name [str] - name of the transform
        source_names_after_transform [list[str]] - name of sources after transform
    Returns:
        list[list[int]]: the timepoint transformation

    """

    ds = read_dataset_metadata(dataset_folder)
    transform = {"sources": [source]}

    source_type = list(ds['sources'][source].keys())[0]
    target_type = list(ds['sources'][target].keys())[0]

    if source_names_after_transform is not None:
        assert len(source_names_after_transform) == 1
        transform["sourceNamesAfterTransform"] = source_names_after_transform

    targetData = ds['sources'][target][target_type]['imageData']
    target_times = get_timepoints(targetData, dataset_folder)

    imageData = ds['sources'][source][source_type]['imageData']
    timepts = get_timepoints(imageData, dataset_folder)

    if not sourceidx:
        sourceidx = list(range(len(timepts)))

    if not targetidx:
        targetidx = list(range(len(target_times)))

    if not name:
        name = source + '_timepoints'

    t_trafo = list()

    for i, t_idx in enumerate(targetidx):
        if t_idx < 0:
            t_idx += len(target_times)

        s_idx = sourceidx[round(i / len(targetidx) * len(sourceidx))]

        t_trafo.append([t_idx, s_idx])

    transform['keep'] = keep
    transform['name'] = name
    transform['parameters'] = t_trafo

    return {"timepoints": transform}


def get_ghosts_view(source, dataset_folder, target=None, sourceidx=None, targetidx=None,
                       start_idx=-5, start_opacity=0.2, end_opacity=1, menu_name=None):
    """
    Creates a set of ghost view displays that display earlier timepoints of one source mapped to later timepoints
    of a target source

    Arguments:
        source [str] - name of the source to be mapped
        dataset_folder [str] - the folder for this dataset
        target [str] - name of the target source if not provided use source
        sourceidx [list[int]] - indeces of the source to be mapped
        targetidx [list[int]] - subset of target indeces
        start_idx [int] - starting index for extraction (if sourceidx not provided)
        start_opacity [float] - starting opacity for the ghost images
        end_opacity [float] - opacity for the last ghost image
        menu_name [str] - menu name
    Returns:
        list[list[int]]: the timepoint transformation

    """

    ds = read_dataset_metadata(dataset_folder)

    source_type = list(ds['sources'][source].keys())[0]

    imageData = ds['sources'][source][source_type]['imageData']
    timepts = get_timepoints(imageData, dataset_folder)

    if not menu_name:
        menu_name = 'ghosts'

    if not sourceidx:
        sourceidx = list(range(len(timepts)))[start_idx:]

    if not target:
        target = source

    target_type = list(ds['sources'][target].keys())[0]

    targetData = ds['sources'][target][target_type]['imageData']
    target_times = get_timepoints(targetData, dataset_folder)

    if not targetidx:
        # last frame only
        targetidx = [len(target_times) - 1]

    source_displays = list()
    region_displays = list()
    # TODO propagate existing source transforms
    t_trafos = list()
    names = list()

    for s_idx, step in enumerate(sourceidx):
        for targetframe in targetidx:
            thisname = source + "_tp_" + str(step) + "-to-" + str(targetframe)
            names.append(thisname)
            thistrafo = get_timepoints_transform(source, dataset_folder, target, sourceidx=[step],
                                                 targetidx=targetidx, source_names_after_transform=[thisname])

            t_trafos.append(thistrafo)

            opacity = s_idx / (max(len(sourceidx),2) - 1) * (end_opacity - start_opacity) + start_opacity

            if source in ds['views'].keys():
                s_disp = ds['views'][source]['sourceDisplays'][0]

                if 'imageDisplay' in s_disp.keys():
                    imdisp = dict(s_disp['imageDisplay'])
                    kwargs = dict()
                    additional_image_kwargs = ["blendingMode",
                                               "resolution3dView",
                                               "showImagesIn3d",
                                               "visible"]
                    for kwarg_name in additional_image_kwargs:
                        kwarg_val = imdisp.pop(kwarg_name, None)
                        if kwarg_val is not None:
                            kwargs[kwarg_name] = kwarg_val

                    if 'opacity' in s_disp['imageDisplay'].keys():
                        opacity *= s_disp['imageDisplay']['opacity']

                    source_displays.append(get_image_display(thistrafo["timepoints"]["sourceNamesAfterTransform"][0],
                                                             thistrafo["timepoints"]["sourceNamesAfterTransform"],
                                                             opacity=round(opacity, 4),
                                                             color=s_disp['imageDisplay']['color'],
                                                             contrastLimits=s_disp['imageDisplay']['contrastLimits'],
                                                             **kwargs
                                                             ))
                elif 'regionDisplay' in s_disp.keys():
                    regdisp = dict(s_disp['regionDisplay'])
                    kwargs = dict()
                    additional_region_kwargs = ["additionalTables"
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
                    for kwarg_name in additional_region_kwargs:
                        kwarg_val = regdisp.pop(kwarg_name, None)
                        if kwarg_val is not None:
                            kwargs[kwarg_name] = kwarg_val

                    if 'opacity' in s_disp['regionDisplay'].keys():
                        opacity *= s_disp['regionDisplay']['opacity']

                    region_displays.append(get_region_display(thistrafo["timepoints"]["sourceNamesAfterTransform"][0],
                                                              thistrafo["timepoints"]["sourceNamesAfterTransform"],
                                                              opacity=round(opacity, 4),
                                                              lut=s_disp['regionDisplay']["lut"],
                                                              table_source=s_disp['regionDisplay']["tableSource"],
                                                              **kwargs
                                                              ))
                elif 'segmentationDisplay' in s_disp.keys():
                    segdisp = dict(s_disp['segmentationDisplay'])
                    kwargs = dict()
                    additional_seg_kwargs = ["boundaryThickness", "colorByColumn",
                                             "randomColorSeed", "resolution3dView",
                                             "selectedSegmentIds", "showAsBoundaries",
                                             "showSelectedSegmentsIn3d", "showTable",
                                             "additionalTables", "valueLimits", "visible",
                                             "opacityNotSelected", "selectionColor"]
                    for kwarg_name in additional_seg_kwargs:
                        kwarg_val = segdisp.pop(kwarg_name, None)
                        if kwarg_val is not None:
                            kwargs[kwarg_name] = kwarg_val

                    if 'opacity' in s_disp['segmentationDisplay'].keys():
                        opacity *= s_disp['segmentationDisplay']['opacity']

                    source_displays.append(get_segmentation_display(
                        thistrafo["timepoints"]["sourceNamesAfterTransform"][0],
                        thistrafo["timepoints"]["sourceNamesAfterTransform"],
                        opacity=round(opacity, 4),
                        lut=s_disp['segmentationDisplay']["lut"],
                        **kwargs
                    ))

            else:
                source_displays.append(get_image_display(thistrafo["timepoints"]["sourceNamesAfterTransform"][0],
                                                         thistrafo["timepoints"]["sourceNamesAfterTransform"],
                                                         opacity=round(opacity, 4)
                                                         ))

    if region_displays == []:
        region_displays = None

    view = get_view(names, [list(ds['sources'][source].keys())[0]] * len(names), [[name] for name in names],
                    source_displays, False, menu_name,
                    source_transforms=t_trafos,
                    region_displays=region_displays)

    return view