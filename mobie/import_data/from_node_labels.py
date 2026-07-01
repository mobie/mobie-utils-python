"""Functionality for importing a segmentation from a fragment segmentation plus a node-label
assignment (a fragment -> segment id mapping, e.g. the output of a graph-based segmentation) into MoBIE.
"""
from typing import Dict, List, Optional, Sequence, Tuple, Union

import numpy as np
from bioimage_py import open_source
from bioimage_py.segmentation import relabel

from .utils import (_create_level, _open_storage, _remove_output,
                    add_max_id, downscale, get_scale_key)
from ..utils import get_run_config


def _load_node_labels(node_label_path: str,
                      node_label_key: str) -> Union[np.ndarray, Dict[int, int]]:
    """Load the fragment -> segment assignment and normalize it into a relabeling that
    :func:`bioimage_py.segmentation.relabel` accepts.

    A 1d dense array (``labeling[old_id] = new_id``) is returned as is; a 2d assignment table of
    ``(old_id, new_id)`` pairs -- stored either as ``(N, 2)`` rows or ``(2, N)`` columns -- is turned
    into a ``{old_id: new_id}`` dict.
    """
    labels = np.asarray(open_source(node_label_path, node_label_key)[...])
    if labels.ndim == 1:
        return labels
    if labels.ndim == 2:
        if labels.shape[1] == 2:
            old, new = labels[:, 0], labels[:, 1]
        elif labels.shape[0] == 2:
            old, new = labels[0, :], labels[1, :]
        else:
            raise ValueError(f"Invalid shape for 2d node labels: {labels.shape}")
        return {int(o): int(n) for o, n in zip(old, new)}
    raise ValueError(f"Expect 1d or 2d node labels, got {labels.ndim}d")


def _write_segmentation(in_path, in_key, out_path, out_key,
                        node_label_path, node_label_key,
                        chunks, metadata_format,
                        target, max_jobs, tmp_folder):
    """Apply the node-label assignment to the fragment segmentation, writing the relabeled scale-0
    dataset to ``out_path/out_key``."""
    job_type, job_config, num_workers = get_run_config(target, max_jobs, tmp_folder)
    labeling = _load_node_labels(node_label_path, node_label_key)

    src = open_source(in_path, in_key)
    # overwrite any previous conversion of this segmentation at the output location.
    _remove_output(out_path)
    with _open_storage(out_path, metadata_format, mode="a") as f:
        # segment ids can exceed the fragment-id range, so store the relabeled output as uint64.
        ds = _create_level(f, metadata_format, 0, src.shape, chunks, np.dtype("uint64"))
        # relabel is a disjoint per-block point op; block_shape == the output chunks keeps
        # concurrent block writes safe (same idiom as downscale's copy calls).
        relabel(src, labeling, output=ds,
                block_shape=tuple(int(c) for c in ds.chunks),
                job_type=job_type, job_config=job_config, num_workers=num_workers)


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
    """Import segmentation data into MoBIE format from a fragment segmentation and a node-label assignment.

    The input fragment segmentation is relabeled by applying the node-label assignment (a mapping from
    fragment ids to segment ids) and then converted into a MoBIE multiscale pyramid.

    Args:
        in_path: The input fragment segmentation to be added.
        in_key: The key of the fragment segmentation to be added.
        out_path: The path of the output segmentation.
        node_label_path: The path to the node labels (the fragment -> segment id assignment).
        node_label_key: The key to the node labels (= internal file path).
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
    if file_format in ("bdv", "bdv.hdf5") and target == "slurm":
        raise ValueError(
            "The bdv.hdf5 format does not support distributed (slurm) writing. "
            "Use target='local' or a different file format."
        )

    out_key = get_scale_key(file_format)

    _write_segmentation(in_path, in_key, out_path, out_key,
                        node_label_path, node_label_key,
                        chunks, metadata_format=file_format,
                        target=target, max_jobs=max_jobs, tmp_folder=tmp_folder)

    downscale(out_path, out_key, out_path,
              resolution, scale_factors, chunks,
              tmp_folder, target, max_jobs, block_shape,
              library="vigra", library_kwargs={"order": 0},
              unit=unit, source_name=source_name,
              metadata_format=file_format)

    # relabel does not write the maxId attribute (the cluster_tools Write task used to), so compute
    # it from the relabeled output (the max segment id actually present).
    add_max_id(out_path, out_key, out_path, out_key, tmp_folder, target, max_jobs)
