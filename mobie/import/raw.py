import os
import json
import luigi

from cluster_tools.downscaling import DownscalingWorkflow


def import_raw_volume(in_path, in_key, out_path,
                      resolution, scale_factors, chunks,
                      tmp_folder, target, max_jobs,
                      block_shape=None):
    task = DownscalingWorkflow

    config_dir = os.path.join(tmp_folder, 'configs')
    os.makedirs(config_dir, exist_ok=True)

    configs = DownscalingWorkflow.get_config()
    global_conf = configs['global']
    block_shape = chunks if block_shape is None else block_shape
    global_conf.update({'block_shape': block_shape})
    with open(os.path.join(config_dir, 'global.config'), 'w') as f:
        json.dump(global_conf, f)

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
