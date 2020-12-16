import os
import json
import luigi

from cluster_tools.write import WriteLocal, WriteSlurm
from .util import downscale
from ..config import write_global_config


def _write_segmentation(in_path, in_key,
                        out_path, out_key,
                        node_label_path, node_label_key,
                        chunks, tmp_folder, max_jobs, target,
                        block_shape=None):
    task = WriteLocal if target == 'local' else WriteSlurm

    config_dir = os.path.join(tmp_folder, 'configs')
    write_global_config(config_dir,
                        block_shape=chunks if block_shape is None else block_shape)

    task_config = task.default_task_config()
    task_config.update({'chunks': chunks})
    with open(os.path.join(config_dir, 'write.config'), 'w') as f:
        json.dump(task_config, f)

    t = task(input_path=in_path, input_key=in_key,
             output_path=out_path, output_key=out_key,
             assignment_path=node_label_path, assignment_key=node_label_key,
             identifier='from_node_labels',
             tmp_folder=tmp_folder, config_dir=config_dir, max_jobs=max_jobs)
    ret = luigi.build([t], local_scheduler=True)
    assert ret, "Writeing segmentation failed"


def import_segmentation_from_node_labels(in_path, in_key, out_path,
                                         node_label_path, node_label_key,
                                         resolution, scale_factors, chunks,
                                         tmp_folder, target, max_jobs,
                                         block_shape=None):
    """ Import segmentation data into mobie format from a paintera dataset

    Arguments:
        in_path [str] - input paintera dataset to be added.
        in_key [str] - key of the paintera dataset to be added.
        out_path [str] - where to add the segmentation.
        node_label_path [str] - path to node labels (default: None)
        node_label_key [str] - key to node labels (default: None)
        resolution [list[float]] - resolution in micrometer
        scale_factors [list[list[int]]] - scale factors used for down-sampling the data
        chunks [tuple[int]] - chunks of the data to be added
        tmp_folder [str] - folder for temporary files
        target [str] - computation target
        max_jobs [int] - number of jobs
        block_shape [tuple[int]] - block shape used for computation.
            By default, same as chunks. (default:None)
    """

    out_key = 'setup0/timepoint0/s0'

    _write_segmentation(in_path, in_key,
                        out_path, out_key,
                        node_label_path, node_label_key,
                        chunks, tmp_folder=tmp_folder,
                        max_jobs=max_jobs, target=target,
                        block_shape=block_shape)

    downscale(out_path, out_key, out_path,
              resolution, scale_factors, chunks,
              tmp_folder, target, max_jobs, block_shape,
              library='vigra', library_kwargs={'order': 0})
