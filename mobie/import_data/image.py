"""Functionality to convert image data into a MoBIE compatible format.
"""
import multiprocessing as mp
from typing import List, Optional, Sequence, Tuple
from .utils import downscale, ensure_volume


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
    int_to_uint: bool = False,
    channel: Optional[int] = None,
    use_memmap: bool = False,
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
        int_to_uint Whether to convert data stored as signed integer to unsigned integer.
        channel: The channel to load from the data. Currently only supported for the ome.zarr format.
        use_memmap: Whether the input is a tif file that can be memmaped.
    """
    # we allow 2d data for ome.zarr file format
    if file_format != "ome.zarr":
        in_path, in_key = ensure_volume(in_path, in_key, tmp_folder, chunks)
    downscale(in_path, in_key, out_path,
              resolution, scale_factors, chunks,
              tmp_folder, target, max_jobs, block_shape,
              library="skimage", unit=unit, source_name=source_name,
              metadata_format=file_format, int_to_uint=int_to_uint,
              channel=channel, use_memmap=use_memmap)
