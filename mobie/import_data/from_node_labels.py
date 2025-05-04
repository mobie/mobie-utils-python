"""Functionality for importing segmentations from paintera into MoBIE.
"""
import os
from typing import List, Optional, Sequence, Tuple

import json
import luigi

from cluster_tools.write import WriteLocal, WriteSlurm
from .utils import downscale, get_scale_key
from ..utils import write_global_config


def _write_segmentation(in_path, in_key,
                        out_path, out_key,
                        node_label_path, node_label_key,
                        chunks, tmp_folder, max_jobs, target,
                        block_shape=None):
    task = WriteLocal if target == "local" else WriteSlurm

    config_dir = os.path.join(tmp_folder, "configs")
    write_global_config(config_dir,
                        block_shape=chunks if block_shape is None else block_shape)

    task_config = task.default_task_config()
    task_config.update({"chunks": chunks})
    with open(os.path.join(config_dir, "write.config"), "w") as f:
        json.dump(task_config, f)

    t = task(input_path=in_path, input_key=in_key,
             output_path=out_path, output_key=out_key,
             assignment_path=node_label_path, assignment_key=node_label_key,
             identifier="from_node_labels",
             tmp_folder=tmp_folder, config_dir=config_dir, max_jobs=max_jobs)
    ret = luigi.build([t], local_scheduler=True)
    assert ret, "Writeing segmentation failed"


def import_segmentation_from_node_labels(
    in_path: str,
    in_key: str,
    out_path: str,
    node_label_path: str,
    node_label_key: str,
    resolution: Sequence[float],
    scale_factors: List[List[int]],
    chunks: Sequence[int],
    tmp_folder: str,
    target: str,
    max_jobs: int,
    block_shape: Optional[Tuple[int, int, int]] = None,
    unit: str = "micrometer",
    source_name: Optional[str] = None,
    file_format: str = "ome.zarr",
) -> None:
    """Import segmentation data into MoBIE format from a paintera dataset.

    Args:
        in_path: The input paintera dataset to be added.
        in_key: The key of the paintera dataset to be added.
        out_path: The path of the output segmentation.
        node_label_path: The path to the paintera node labels.
        node_label_key: The key to the paintera node labels (= internal file path).
        resolution: The resolution in physical units.
        scale_factors: The scale factors used for down-sampling the data.
        chunks: The chunks of the data to be added.
        tmp_folder: The folder for temporary files
        target: The computation target.
        max_jobs: The number of jobs for parallelization.
        block_shape: The block shape to use for computation. By default, same as chunks.
        unit: The physical unit of the coordinate system.
        source_name: The name of the source.
        file_format: The output file format.
    """

    out_key = get_scale_key(file_format)

    _write_segmentation(in_path, in_key,
                        out_path, out_key,
                        node_label_path, node_label_key,
                        chunks, tmp_folder=tmp_folder,
                        max_jobs=max_jobs, target=target,
                        block_shape=block_shape)

    downscale(out_path, out_key, out_path,
              resolution, scale_factors, chunks,
              tmp_folder, target, max_jobs, block_shape,
              library="vigra", library_kwargs={"order": 0},
              unit=unit, source_name=source_name,
              metadata_format=file_format)
