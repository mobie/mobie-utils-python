import numpy as np

from .dataset_metadata import read_dataset_metadata
from .source_metadata import get_timepoints
from .view_metadata import get_image_display


def get_timepoints_transform(sources, dataset_folder, start=0, end=1):
    ds = read_dataset_metadata(dataset_folder)
    timepoints = dict()
    len_time = dict()

    for source in sources:
        imageData = ds['sources'][source]['image']['imageData']
        timept = get_timepoints(imageData, dataset_folder)
        timepoints[source] = timept
        len_time[source] = len(timept)

    if sum([val > 1 for val in len_time.values()]) > 1:
        raise ValueError('Only one multi-timepoint source supported.')

    t_max = max(len_time.values())

    max_key = [key for key, value in len_time.items() if value == t_max][0]



    for source in sources:






    return {}
