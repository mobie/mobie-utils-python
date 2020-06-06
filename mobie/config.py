import json
import os
from cluster_tools.cluster_tasks import BaseClusterTask


def write_global_config(config_folder, block_shape=None, roi_begin=None, roi_end=None, qos=None):
    os.makedirs(config_folder, exist_ok=True)

    conf_path = os.path.join(config_folder, 'global.config')
    if os.path.exists(conf_path):
        with open(conf_path) as f:
            global_config = json.load(f)
    else:
        global_config = BaseClusterTask.default_global_config()

    if block_shape is not None:
        if len(block_shape) != 3:
            raise ValueError("Invalid block_shape given")
        global_config['block_shape'] = block_shape

    if roi_begin is not None:
        if len(block_shape) != 3:
            raise ValueError("Invalid roi_begin given")
        global_config['roi_begin'] = roi_begin

    if roi_end is not None:
        if len(block_shape) != 3:
            raise ValueError("Invalid roi_end given")
        global_config['roi_end'] = roi_end

    if qos is not None:
        global_config['qos'] = qos

    with open(conf_path, 'w') as f:
        json.dump(global_config, f)
