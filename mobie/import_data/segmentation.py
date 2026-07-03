"""Functionality for converting segmentation data into a format compatible with MoBIE.
"""
from typing import List, Optional, Sequence, Tuple
from .utils import add_max_id, downscale, get_scale_key


def import_segmentation(
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
    with_max_id: bool = True,
    unit: str = "micrometer",
    source_name: Optional[str] = None,
    file_format: str = "ome.zarr",
    ome_zarr_version: str = "0.4",
    shards: Optional[Sequence[int]] = None,
) -> None:
    """Import segmentation data into a MoBIE-compatible format.

    Args:
        in_path: The input segmentation to be added.
        in_key: The key of the segmentation to be added.
        out_path: The output path where the converted segmentation is saved.
        resolution: The resolution of the data in physical units.
        scale_factors: The scale factors used for down-sampling the data.
        chunks: The chunks of the data to be added.
        tmp_folder: The folder for temporary files.
        target: The computation target.
        max_jobs: The number of jobs for parallelization.
        block_shape: The block shape to use for computation. By default, same as chunks.
        with_max_id: Whether to add the max id attribute.
        unit: The physical unit of the coordinate system.
        source_name: The name of the source.
        file_format: The output file format.
        ome_zarr_version: The ome.zarr / NGFF version to write ('0.4' -> zarr v2, '0.5' -> zarr v3).
        shards: The shard shape for zarr v3 sharding. Only valid for ome.zarr v0.5.
    """
    # 2d input is promoted to 3d on the fly inside downscale for the bdv formats (ome.zarr keeps 2d).
    downscale(in_path, in_key, out_path,
              resolution, scale_factors, chunks,
              tmp_folder, target, max_jobs, block_shape,
              library="vigra", library_kwargs={"order": 0},
              unit=unit, source_name=source_name,
              metadata_format=file_format,
              ome_zarr_version=ome_zarr_version, shards=shards)

    if with_max_id:
        out_key = get_scale_key(file_format)
        add_max_id(in_path, in_key, out_path, out_key, tmp_folder, target, max_jobs)
