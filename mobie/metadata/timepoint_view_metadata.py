from .dataset_metadata import read_dataset_metadata
from .source_metadata import get_timepoints
from .view_metadata import get_image_display


def get_timepoints_transform(source, target, dataset_folder, sourceidx=None, targetidx=None):
    """
    Creates a timepoint transformation mapping timepoints from one source to timepoints of a target source

    Arguments:
        source [str] - name of the source to be mapped
        target [str] - name of the target source
        dataset_folder [str] - the folder for this dataset
        sourceidx [list[int]] - indeces of the source to be mapped
        targetidx [list[int]] - subset of target indeces

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

    t_trafo = list()

    for idx in targetidx:
        s_idx = sourceidx[round(idx/len(targetidx) * len(sourceidx))]

        t_trafo.append([idx, s_idx])







    return {}
