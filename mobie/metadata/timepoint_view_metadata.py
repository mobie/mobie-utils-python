from .dataset_metadata import read_dataset_metadata
from .source_metadata import get_timepoints
from .view_metadata import get_image_display, get_region_display


def get_timepoints_transform(source, dataset_folder, target, sourceidx=None, targetidx=None, keep=False, name=None):
    """
    Creates a timepoint transformation mapping timepoints from one source to timepoints of a target source

    Arguments:
        source [str] - name of the source to be mapped
        dataset_folder [str] - the folder for this dataset
        target [str] - name of the target source
        sourceidx [list[int]] - indeces of the source to be mapped
        targetidx [list[int]] - subset of target indeces
        keep [bool] - whether other timepoints of the source are still available
        name [str] - name of ther transform
    Returns:
        list[list[int]]: the timepoint transformation

    """

    ds = read_dataset_metadata(dataset_folder)

    targetData = ds['sources'][target]['image']['imageData']
    target_times = get_timepoints(targetData, dataset_folder)

    imageData = ds['sources'][source]['image']['imageData']
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

        s_idx = sourceidx[round(i/len(targetidx) * len(sourceidx))]

        t_trafo.append([t_idx, s_idx])

    transform = {"sources":[source]}
    transform['keep'] = keep
    transform['name'] = name
    transform['parameters'] = t_trafo

    return {"timepoints":transform}

def create_ghosts(source, dataset_folder, target=None, sourceidx=None, targetidx=None,
                  start_idx=-5, start_opacity=0.2, end_opacity=1):
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
    Returns:
        list[list[int]]: the timepoint transformation

    """

    ds = read_dataset_metadata(dataset_folder)

    imageData = ds['sources'][source]['image']['imageData']
    timepts = get_timepoints(imageData, dataset_folder)

    if not sourceidx:
        sourceidx = list(range(len(timepts)))[start_idx:]

    if not target:
        target = source

    targetData = ds['sources'][target]['image']['imageData']
    target_times = get_timepoints(targetData, dataset_folder)

    if not targetidx:
        # last frame only
        targetidx = [len(target_times) - 1]

    s_displays = list()
    t_trafos = list()

    for s_idx,step in enumerate(sourceidx):
        for targetframe in targetidx:
            thistrafo = get_timepoints_transform(source, dataset_folder, target, sourceidx=[step], targetidx=targetidx)
            thistrafo["sourceNamesAfterTransform"] = source + "_tp_" + str(step) + "-to-" + str(targetframe)
            t_trafos.append(thistrafo)

            opacity = s_idx/(len(sourceidx)-1) * (end_opacity-start_opacity) + start_opacity

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

                    s_displays.append(get_image_display(thistrafo["sourceNamesAfterTransform"],
                                                        [thistrafo["sourceNamesAfterTransform"]],
                                                        opacity=f'{opacity:.4f}',
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

                    s_displays.append(get_region_display(thistrafo["sourceNamesAfterTransform"],
                                                         [thistrafo["sourceNamesAfterTransform"]],
                                                         opacity=f'{opacity:.4f}',
                                                         table_source=s_disp['regionDisplay']["tableSource"],
                                                         **kwargs
                                                         ))

            else:
                s_displays.append(get_image_display(thistrafo["sourceNamesAfterTransform"],
                                                        [thistrafo["sourceNamesAfterTransform"]],
                                                        opacity=f'{opacity:.4f}'))

    return s_displays, t_trafos