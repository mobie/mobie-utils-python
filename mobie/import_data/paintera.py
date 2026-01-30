"""Functionality for converting paintera data into a segmentation compatible with MoBIE.
"""
import os
from typing import Dict, List, Optional, Sequence, Tuple

from elf.io import open_file, is_z5py, is_group
from .utils import downscale, add_max_id

try:
    from paintera_tools import serialize_from_commit, postprocess
except ImportError:
    serialize_from_commit = None


def is_paintera(path, key):
    """@private
    """
    expected_keys = {"data",
                     "fragment-segment-assignment",
                     "unique-labels",
                     "label-to-block-mapping"}
    with open_file(path, "r") as f:
        if not is_z5py(f):
            return False
        g = f[key]
        if not is_group(g):
            return False
        keys = set(g.keys())
        if len(expected_keys - keys) > 0:
            return False
    return True


def import_segmentation_from_paintera(
    in_path: str,
    in_key: str,
    out_path: str,
    resolution: Sequence[float],
    scale_factors: List[List[int]],
    chunks: Sequence[int],
    tmp_folder: str,
    target: str,
    max_jobs: int,
    block_shape: Optional[Tuple[int, int, int]] = None,
    postprocess_config: Optional[Dict] = None,
    map_to_background: Optional[Sequence[int]] = None,
    unit: str = "micrometer",
    source_name: Optional[str] = None,
) -> None:
    """Import segmentation data into mobie format from a paintera dataset

    Args:
        in_path: The input paintera dataset to be added.
        in_key: The key of the paintera dataset to be added.
        out_path: The output path for saving the converted segmentation.
        resolution: The resolution in physical units.
        scale_factors: The scale factors to use for down-sampling the data.
        chunks: The chunks of the data to be added.
        tmp_folder: The folder for temporary files.
        target: The computation target.
        max_jobs: The number of jobs for parallelization.
        block_shape: The block shape to use for computation. By default, same as chunks.
        postprocess_config: The config for segmentation post-processing.
        map_to_background: Optional ids to be mapped to background label.
        unit: The physical unit of the coordinate system.
        source_name: The name of the source.
    """
    if serialize_from_commit is None:
        msg = """Importing a segmentation from paintera is only possible wit paintera_tools:
        https://github.com/constantinpape/paintera_tools
        """
        raise AttributeError(msg)

    out_key = "setup0/timepoint0/s0"
    # run post-processing if specified for this segmentation name
    if postprocess_config is not None:
        boundary_path = postprocess_config["BoundaryPath"]
        boundary_key = postprocess_config["BoundaryKey"]

        min_segment_size = postprocess_config.get("MinSegmentSize", None)
        max_segment_number = postprocess_config.get("MaxSegmentNumber", None)

        label_segmentation = postprocess_config["LabelSegmentation"]
        tmp_postprocess = os.path.join(tmp_folder, "postprocess_paintera")

        print("Run postprocessing:")
        if label_segmentation:
            print("with connected components")
        if max_segment_number is not None:
            print("With max segment number:", max_segment_number)
        if min_segment_size is not None:
            print("With min segment size:", min_segment_size)

        postprocess(in_path, in_key,
                    boundary_path, boundary_key,
                    tmp_folder=tmp_postprocess,
                    target=target, max_jobs=max_jobs,
                    n_threads=8, size_threshold=min_segment_size,
                    target_number=max_segment_number,
                    label=label_segmentation,
                    output_path=out_path, output_key=out_key)

    else:
        # export segmentation from in commit for all scales
        serialize_from_commit(in_path, in_key, out_path, out_key, tmp_folder,
                              max_jobs, target, relabel_output=True,
                              map_to_background=map_to_background)

    downscale(out_path, out_key, out_path,
              resolution, scale_factors, chunks,
              tmp_folder, target, max_jobs, block_shape,
              library="vigra", library_kwargs={"order": 0},
              unit=unit, source_name=source_name)

    add_max_id(in_path, in_key, out_path, out_key,
               tmp_folder, target, max_jobs)
