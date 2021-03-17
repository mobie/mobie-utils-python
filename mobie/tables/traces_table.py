import os
import numpy as np
import pandas as pd

from elf.io import open_file, is_h5py
from pybdv.metadata import get_data_path
from pybdv.util import get_key
from tqdm import tqdm

from .util import remove_background_label_row
from ..import_data.traces import parse_traces, vals_to_coords


def compute_trace_default_table(input_folder, table_path, resolution, seg_infos={}):
    """ Compute the default table for the input traces, consisting of the
    attributes necessary to enable tables in the mobie-fiji-viewer.

    Arguments:
        input_folder [str] - folder with the traces in nmx or swc format.
        table_path [str] - where to save the table
        resolution [list[float]] - resolution of the traces in microns
        seg_infos [dict] - additional segmentations included in the table computation (defalt: {})
    """

    traces = parse_traces(input_folder)

    files = {}
    datasets = {}
    for seg_name, seg_info in seg_infos.items():

        seg_path = seg_info['path']
        if seg_path.endswith('.xml'):
            seg_path = get_data_path(seg_path, return_absolute_path=True)
        seg_scale = seg_info['scale']
        is_h5 = is_h5py(seg_path)
        seg_key = get_key(is_h5, time_point=0, setup_id=0, scale=seg_scale)
        f = open_file(seg_path, 'r')
        ds = f[seg_key]

        if len(files) == 0:
            ref_shape = ds.shape
        else:
            assert ds.shape == ref_shape, "%s, %s" % (str(ds.shape), str(ref_shape))

        files[seg_name] = f
        datasets[seg_name] = ds

    table = []
    for nid, vals in tqdm(traces.items()):

        coords = vals_to_coords(vals, resolution)
        bb_min = coords.min(axis=0)
        bb_max = coords.max(axis=0) + 1

        # get spatial attributes
        anchor = coords[0].astype('float32') * resolution / 1000.
        bb_min = bb_min.astype('float32') * resolution / 1000.
        bb_max = bb_max.astype('float32') * resolution / 1000.

        # get cell and nucleus ids
        point_slice = tuple(slice(int(c), int(c) + 1) for c in coords[0])
        # attributes:
        # label_id
        # anchor_x anchor_y anchor_z
        # bb_min_x bb_min_y bb_min_z bb_max_x bb_max_y bb_max_z
        # n_points + seg ids
        attributes = [nid, anchor[2], anchor[1], anchor[0],
                      bb_min[2], bb_min[1], bb_min[0],
                      bb_max[2], bb_max[1], bb_max[0],
                      len(coords)]

        for ds in datasets.values():
            seg_id = ds[point_slice][0, 0, 0]
            attributes += [seg_id]

        table.append(attributes)

    for f in files.values():
        f.close()

    table = np.array(table, dtype='float32')
    header = ['label_id', 'anchor_x', 'anchor_y', 'anchor_z',
              'bb_min_x', 'bb_min_y', 'bb_min_z',
              'bb_max_x', 'bb_max_y', 'bb_max_z',
              'n_points']
    header += ['%s_id' % seg_name for seg_name in seg_infos]

    table_folder = os.path.split(table_path)[0]
    os.makedirs(table_folder, exist_ok=True)

    table = pd.DataFrame(table, columns=header)
    table = remove_background_label_row(table)
    table.to_csv(table_path, index=False, sep='\t')
