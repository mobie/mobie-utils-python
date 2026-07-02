"""Functionality to convert image data into a MoBIE compatible format.
"""
import multiprocessing as mp
from typing import List, Optional, Sequence, Tuple
from .utils import downscale


def import_image_data(
    in_path,
    in_key,
    out_path,
    resolution: Sequence[float],
    scale_factors: List[List[int]],
    chunks: Sequence[int],
    tmp_folder: Optional[str] = None,
    target: str = "local",
    max_jobs: int = mp.cpu_count(),
    block_shape: Optional[Tuple[int, int, int]] = None,
    unit: str = "micrometer",
    source_name: Optional[str] = None,
    file_format: str = "ome.zarr",
    channel: Optional[int] = None,
) -> None:
    """Convert image data into a format supported by MoBIE.

    Args:
        in_path: The input data to be added.
        in_key: The key of the input data to be added.
        out_path: The output path for the converted data.
        resolution: The resolution of the data in physical units.
        scale_factors: The scale factors used for down-sampling the data.
        chunks: The chunks of the data to be added.
        tmp_folder: The folder for temporary files.
        target: The computation target.
        max_jobs: The number of jobs.
        block_shape: The block shape to use for computation. By default, same as chunks.
        unit: The physical unit of the coordinate system.
        source_name: The name of the source.
        file_format: The file format the data will be converted into.
        channel: The channel to load from the data.
    """
    # 2d input is promoted to 3d on the fly inside downscale for the bdv formats (ome.zarr keeps 2d).
    downscale(in_path, in_key, out_path,
              resolution, scale_factors, chunks,
              tmp_folder, target, max_jobs, block_shape,
              library="skimage", unit=unit, source_name=source_name,
              metadata_format=file_format, channel=channel)
