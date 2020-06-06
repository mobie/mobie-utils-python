import os
import json
import luigi

from cluster_tools.downscaling import DownscalingWorkflow
from ..config import write_global_config


def import_raw_volume(in_path, in_key, out_path,
                      resolution, scale_factors, chunks,
                      tmp_folder, target, max_jobs,
                      block_shape=None):
    task = DownscalingWorkflow

    block_shape = chunks if block_shape is None else block_shape
    config_dir = os.path.join(tmp_folder, 'configs')
    write_global_config(config_dir, block_shape=block_shape)

    configs = DownscalingWorkflow.get_config()
    conf = configs['copy_volume']
    conf.update({'chunks': chunks})
    with open(os.path.join(config_dir, 'copy_volume.config'), 'w') as f:
        json.dump(conf, f)

    conf = configs['downscaling']
    conf.update({'chunks': chunks, 'library': 'skimage'})
    with open(os.path.join(config_dir, 'downscaling.config'), 'w') as f:
        json.dump(conf, f)

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
    assert ret, "Importing raw data failed"
