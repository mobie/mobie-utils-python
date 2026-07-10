import json
import multiprocessing
import os

import luigi
import numpy as np
import pandas as pd
from cluster_tools.evaluation import ObjectIouWorkflow, ObjectViWorkflow
from pybdv.metadata import get_data_path

from ..config import write_global_config
from ..metadata import load_image_dict

METRICS = {
    'iou': ObjectIouWorkflow,
    'voi': ObjectViWorkflow
}


def read_metrics(path, metric):
    with open(path) as f:
        scores = json.load(f)

    if metric == 'iou':  # iou just has a single metric per object
        data = np.array([
            [int(label_id), score] for label_id, score in scores.items()
        ])
        columns = ['label_id', 'iou-score']
    else:  # voi has two metrics per object
        data = np.array([
            [int(label_id), score[0], score[1]] for label_id, score in scores.items()
        ])
        columns = ['label_id', 'voi-split-score', 'voi-merge-score']

    return data, columns


def write_metric_table(table_path, data, columns):
    if os.path.exists(table_path):
        df = pd.read_csv(table_path, sep='\t')

        label_ids = data[:, 0]
        if not np.allclose(label_ids, df['label_id'].values):
            raise RuntimeError("Label ids in metrics table disagree")

        for col_id, col_name in enumerate(columns[1:], 1):
            if col_name in df.columns:
                df[col_name] = data[:, 1]
            else:
                merge_data = np.concatenate([data[:, 0:1], data[:, col_id:col_id+1]], axis=1)
                df = df.merge(pd.DataFrame(merge_data, columns=['label_id', col_name]))
    else:
        df = pd.DataFrame(data, columns=columns)
    df.to_csv(table_path, index=False, sep='\t')


def compute_metrics_table(
    dataset_folder,
    seg_name,
    gt_name,
    metric='iou',
    scale=0,
    tmp_folder=None,
    target='local',
    max_jobs=multiprocessing.cpu_count()
):
    """
    """

    if metric not in METRICS:
        msg = f"Metric {metric} is not supported. Only {list(METRICS.keys())} are supported."
        raise ValueError(msg)
    task = METRICS[metric]

    image_folder = os.path.join(dataset_folder, 'images')
    image_dict = load_image_dict(os.path.join(image_folder, 'images.json'))

    seg_entry = image_dict[seg_name]
    seg_path = os.path.join(image_folder, seg_entry['storage']['local'])
    seg_path = get_data_path(seg_path, return_absolute_path=True)

    gt_entry = image_dict[gt_name]
    gt_path = os.path.join(image_folder, gt_entry['storage']['local'])
    gt_path = get_data_path(gt_path, return_absolute_path=True)

    tmp_folder = f'tmp_metrics_{seg_name}_{gt_name}' if tmp_folder is None else tmp_folder
    config_dir = os.path.join(tmp_folder, 'configs')
    write_global_config(config_dir)

    key = f'setup0/timepoint0/s{scale}'
    out_path = os.path.join(tmp_folder, f'scores_{metric}.json')
    t = task(tmp_folder=tmp_folder, config_dir=config_dir,
             target=target, max_jobs=max_jobs,
             seg_path=seg_path, seg_key=key,
             gt_path=gt_path, gt_key=key,
             output_path=out_path)
    if not luigi.build([t], local_scheduler=True):
        raise RuntimeError("Computing metrics failed.")

    data, columns = read_metrics(out_path, metric)

    table_dir = os.path.join(dataset_folder, seg_entry['tableFolder'])
    table_path = os.path.join(table_dir, 'metrics.csv')

    write_metric_table(table_path, data, columns)
