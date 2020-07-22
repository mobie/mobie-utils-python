import json
import os

import luigi
from cluster_tools.downscaling import DownscalingWorkflow
from cluster_tools.statistics import DataStatisticsWorkflow
from elf.io import open_file
from ..config import write_global_config


def downscale(in_path, in_key, out_path,
              resolution, scale_factors, chunks,
              tmp_folder, target, max_jobs, block_shape,
              library='vigra', library_kwargs=None):
    task = DownscalingWorkflow

    block_shape = chunks if block_shape is None else block_shape
    config_dir = os.path.join(tmp_folder, 'configs')
    write_global_config(config_dir, block_shape=block_shape)

    configs = DownscalingWorkflow.get_config()
    conf = configs['copy_volume']
    conf.update({'chunks': chunks})
    with open(os.path.join(config_dir, 'copy_volume.config'), 'w') as f:
        json.dump(conf, f)

    ds_conf = configs['downscaling']
    ds_conf.update({'chunks': chunks, 'library': library})
    if library_kwargs is not None:
        ds_conf.update({'library_kwargs': library_kwargs})
    with open(os.path.join(config_dir, 'downscaling.config'), 'w') as f:
        json.dump(ds_conf, f)

    halos = scale_factors
    metadata_format = 'bdv.n5'
    metadata_dict = {'resolution': resolution, 'unit': 'micrometer'}

    t = task(tmp_folder=tmp_folder, config_dir=config_dir,
             target=target, max_jobs=max_jobs,
             input_path=in_path, input_key=in_key,
             scale_factors=scale_factors, halos=halos,
             metadata_format=metadata_format, metadata_dict=metadata_dict,
             output_path=out_path)
    ret = luigi.build([t], local_scheduler=True)
    if not ret:
        raise RuntimeError("Downscaling failed")


def compute_max_id(path, key, tmp_folder, target, max_jobs):
    task = DataStatisticsWorkflow

    stat_path = os.path.join(tmp_folder, 'statistics.json')
    t = task(tmp_folder=tmp_folder, config_dir=os.path.join(tmp_folder, 'configs'),
             target=target, max_jobs=max_jobs,
             path=path, key=key, output_path=stat_path)
    ret = luigi.build([t], local_scheduler=True)
    if not ret:
        raise RuntimeError("Computing max id failed")

    with open(stat_path) as f:
        stats = json.load(f)

    return stats['max']


def add_max_id(in_path, in_key, out_path, out_key,
               tmp_folder, target, max_jobs):
    with open_file(out_path, 'r') as f_out:
        ds_out = f_out[out_key]
        if 'maxId' in ds_out.attrs:
            return

    with open_file(in_path, 'r') as f:
        max_id = f[in_key].attrs.get('maxId', None)

    if max_id is None:
        max_id = compute_max_id(out_path, out_key,
                                tmp_folder, target, max_jobs)

    with open_file(out_path, 'a') as f:
        f[out_key].attrs['maxId'] = int(max_id)
