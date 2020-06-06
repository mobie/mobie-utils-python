import os

import luigi
import numpy as np
import pandas as pd

from cluster_tools.morphology import MorphologyWorkflow
from elf.io import open_file
from ..config import write_global_config


def _table_impl(input_path, input_key, tmp_folder, target, max_jobs):
    task = MorphologyWorkflow

    out_path = os.path.join(tmp_folder, 'data.n5')
    config_folder = os.path.join(tmp_folder, 'configs')

    out_key = 'attributes'
    t = task(tmp_folder=tmp_folder, max_jobs=max_jobs, target=target,
             config_dir=config_folder,
             input_path=input_path, input_key=input_key,
             output_path=out_path, output_key=out_key,
             prefix='attributes', max_jobs_merge=min(32, max_jobs))
    ret = luigi.build([t], local_scheduler=True)
    if not ret:
        raise RuntimeError("Attribute workflow failed")
    return out_path, out_key


def to_csv(input_path, input_key, output_path, resolution,
           anchors=None):
    # load the attributes from n5
    with open_file(input_path, 'r') as f:
        attributes = f[input_key][:]
    label_ids = attributes[:, 0:1]

    # the colomn names
    col_names = ['label_id',
                 'anchor_x', 'anchor_y', 'anchor_z',
                 'bb_min_x', 'bb_min_y', 'bb_min_z',
                 'bb_max_x', 'bb_max_y', 'bb_max_z',
                 'n_pixels']

    # we need to switch from our axis conventions (zyx)
    # to java conventions (xyz)
    res_in_micron = resolution[::-1]

    def translate_coordinate_tuple(coords):
        coords = coords[:, ::-1]
        for d in range(3):
            coords[:, d] *= res_in_micron[d]
        return coords

    # center of mass / anchor points
    com = attributes[:, 2:5]
    if anchors is None:
        anchors = translate_coordinate_tuple(com)
    else:
        assert len(anchors) == len(com)
        assert anchors.shape[1] == 3

        # some of the corrected anchors might not be present,
        # so we merge them with the com here
        invalid_anchors = np.isclose(anchors, 0.).all(axis=1)
        anchors[invalid_anchors] = com[invalid_anchors]
        anchors = translate_coordinate_tuple(anchors)

    # attributes[5:8] = min coordinate of bounding box
    minc = translate_coordinate_tuple(attributes[:, 5:8])
    # attributes[8:11] = min coordinate of bounding box
    maxc = translate_coordinate_tuple(attributes[:, 8:11])

    # NOTE attributes[1] = size in pixel
    # wrie the output table
    data = np.concatenate([label_ids, anchors, minc, maxc, attributes[:, 1:2]], axis=1)
    df = pd.DataFrame(data, columns=col_names)
    df.to_csv(output_path, sep='\t', index=False)

    return label_ids


def compute_default_table(seg_path, seg_key, table_path,
                          resolution, tmp_folder, target, max_jobs,
                          correct_anchors=False):
    """ Compute the default table for the input segmentation, consisting of the
    attributes necessary to enable tables in the mobie-fiji-viewer.

    Arguments:
        seg_path [str] - input path to the segmentation
        seg_key [str] - key to the segmenation
        table_path [str] - path to the output table
        resolution [list[folat]] - resolution of the data in microns
        tmp_folder [str] - folder for temporary files
        target [str] - computation target
        max_jobs [int] - number of jobs
        correct_anchors [bool] - whether to move the anchor points into segmentation objects.
            Anchor points may be outside of objects in case of concave objects. (default: False)
    """

    # prepare cluster tools tasks
    write_global_config(os.path.join(tmp_folder, 'configs'))

    # make base attributes as n5 dataset
    tmp_path, tmp_key = _table_impl(seg_path, seg_key,
                                    tmp_folder, target, max_jobs)

    # TODO implement scalable anchor correction via distance transform maxima
    # correct anchor positions
    if correct_anchors:
        raise NotImplementedError("Anchor correction is not implemented yet")
        # anchors = anchor_correction(seg_path, seg_key,
        #                             tmp_folder, target, max_jobs)
    else:
        anchors = None

    # write output to csv
    label_ids = to_csv(tmp_path, tmp_key, table_path, resolution, anchors)
    return label_ids
