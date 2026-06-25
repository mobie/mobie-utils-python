"""@private
"""
import os
import shutil

import h5py
import numpy as np
import z5py
from bioimage_py import copy, open_source, stats
from bioimage_py.sources import as_source
from bioimage_py.wrapper import ExpandDimsSource, ResizedSource, RoiSource
from elf.io import open_file
from pybdv.downsample import sample_shape

from ..utils import get_run_config
from ._format_metadata import write_format_metadata

# anti-aliasing (gaussian pre-smoothing before downsampling) is only supported for these dtypes.
_ANTI_ALIASING_DTYPES = ("float32", "float64", "uint8", "uint16")


def get_scale_key(file_format, scale=0):
    if file_format == "bdv.n5":
        out_key = f"setup0/timepoint0/s{scale}"
    elif file_format == "bdv.hdf5":
        out_key = f"t00000/s00/{scale}/cells"
    elif file_format in ("ome.zarr", "ome.zarr.s3"):
        out_key = f"s{scale}"
    else:
        raise ValueError("Invalid file-format: {file_format}")
    return out_key


def _open_storage(path, metadata_format, mode="a"):
    """Open the output container with the backend that writes the on-disk format MoBIE expects.

    ome.zarr is written as zarr v2 (`.zarray` + dimension_separator='/') via z5py, bdv.n5 as n5
    via z5py, and bdv.hdf5 via h5py. (`elf.io.open_file` is intentionally not used here because it
    writes zarr v3, which is incompatible with the MoBIE NGFF v0.4 layout.)
    """
    if metadata_format == "ome.zarr":
        return z5py.File(path, mode=mode, dimension_separator="/")
    elif metadata_format == "bdv.n5":
        return z5py.File(path, mode=mode)
    elif metadata_format in ("bdv", "bdv.hdf5"):
        return h5py.File(path, mode=mode)
    raise ValueError(f"Invalid file-format: {metadata_format}")


def _open_data(path, mode="r"):
    """Open existing converted data (for reading values / attributes) with the matching backend."""
    ext = os.path.splitext(path)[1]
    if ext == ".n5":
        return z5py.File(path, mode=mode)
    elif ext in (".zarr", ".zr"):
        return z5py.File(path, mode=mode, dimension_separator="/")
    return h5py.File(path, mode=mode)


def _downsampling_params(library, library_kwargs, dtype):
    """Translate the legacy (vigra/skimage) downscaling options into bioimage-py parameters.

    vigra with order 0 -> nearest-neighbor, no anti-aliasing (label-safe), used for segmentations.
    skimage -> linear interpolation with anti-aliasing, used for intensity images.
    Anti-aliasing is only available for a subset of dtypes; for the others (e.g. signed-integer
    images) we fall back to plain linear interpolation, which works for all dtypes.
    """
    if library_kwargs is not None and "order" in library_kwargs:
        order = int(library_kwargs["order"])
    else:
        order = 0 if library == "vigra" else 1
    anti_aliasing = library == "skimage" and order > 0
    if anti_aliasing and np.dtype(dtype).name not in _ANTI_ALIASING_DTYPES:
        anti_aliasing = False
    return order, anti_aliasing


def _remove_output(out_path):
    """Remove a previous conversion at the output location (data + companion bdv xml)."""
    if os.path.isdir(out_path):
        shutil.rmtree(out_path)
    elif os.path.exists(out_path):
        os.remove(out_path)
    xml_path = os.path.splitext(out_path)[0] + ".xml"
    if os.path.exists(xml_path):
        os.remove(xml_path)


def _validate(ndim, resolution, scale_factors, metadata_format):
    # ome.zarr can also be written in 2d, all other formats require 3d.
    if metadata_format != "ome.zarr" and ndim != 3:
        raise ValueError(f"Expect 3d data for the {metadata_format} format, got ndim={ndim}")
    if len(resolution) != ndim:
        raise ValueError(f"Expect resolution of length {ndim}, got: resolution={resolution}")
    for sf in scale_factors:
        if len(sf) != ndim:
            raise ValueError(f"Expect scale factors of length {ndim}, got: {sf}")


def _create_level(f, metadata_format, scale, shape, chunks, dtype):
    key = get_scale_key(metadata_format, scale)
    shape = tuple(int(s) for s in shape)
    # clip the chunks to the level shape: h5py rejects chunks larger than the data shape, and
    # keeping block_shape == chunks (see below) guarantees safe concurrent block writes.
    level_chunks = tuple(int(min(c, s)) for c, s in zip(chunks, shape))
    return f.create_dataset(key, shape=shape, chunks=level_chunks, dtype=dtype)


def _build_pyramid(f, base, base_shape, scale_factors, metadata_format, chunks, dtype,
                   order, anti_aliasing, run_kwargs):
    prev, prev_shape = base, tuple(int(s) for s in base_shape)
    for level, factor in enumerate(scale_factors, start=1):
        level_shape = tuple(int(s) for s in sample_shape(prev_shape, factor))
        ds = _create_level(f, metadata_format, level, level_shape, chunks, dtype)
        resized = ResizedSource(as_source(prev), level_shape, order=order, anti_aliasing=anti_aliasing)
        copy(resized, output=ds, block_shape=tuple(int(c) for c in ds.chunks), **run_kwargs)
        prev, prev_shape = ds, level_shape


def downscale(in_path, in_key, out_path,
              resolution, scale_factors, chunks,
              tmp_folder, target, max_jobs, block_shape,
              library="vigra", library_kwargs=None,
              metadata_format="ome.zarr",
              unit="micrometer", source_name=None,
              channel=None):
    """Convert input data into a MoBIE multiscale pyramid using bioimage-py and write the metadata.

    Note: the `block_shape` argument is accepted for backwards compatibility but is no longer used;
    write blocks now follow the (per-level) storage chunks, which keeps concurrent writes safe.
    """
    if metadata_format in ("bdv", "bdv.hdf5") and target == "slurm":
        raise ValueError(
            "The bdv.hdf5 format does not support distributed (slurm) writing. "
            "Use target='local' or a different file format."
        )

    job_type, job_config, num_workers = get_run_config(target, max_jobs, tmp_folder)
    run_kwargs = dict(job_type=job_type, job_config=job_config, num_workers=num_workers)

    # downscaling in-place: the scale-0 data already exists at out_path/in_key (e.g. when importing
    # a segmentation from node labels / paintera). In that case we only add the downsampled levels.
    in_place = os.path.abspath(in_path) == os.path.abspath(out_path)

    if in_place:
        with _open_storage(out_path, metadata_format, mode="a") as f:
            base = f[in_key]
            ndim = base.ndim
            _validate(ndim, resolution, scale_factors, metadata_format)
            order, anti_aliasing = _downsampling_params(library, library_kwargs, base.dtype)
            _build_pyramid(f, base, base.shape, scale_factors, metadata_format, chunks,
                           base.dtype, order, anti_aliasing, run_kwargs)
    else:
        src = open_source(in_path, in_key) if in_key else open_source(in_path)
        if channel is not None:
            src = RoiSource(src, roi=(channel,), squeeze=True)
        # the bdv formats require 3d data; promote a 2d source to (1, y, x) on the fly via a wrapper
        # view (ome.zarr keeps 2d data as-is). This replaces the former on-disk temp file.
        if metadata_format != "ome.zarr" and src.ndim == 2:
            src = ExpandDimsSource(src, axis=0)
        ndim = src.ndim
        _validate(ndim, resolution, scale_factors, metadata_format)
        order, anti_aliasing = _downsampling_params(library, library_kwargs, src.dtype)

        # overwrite any previous conversion of this source at the output location.
        _remove_output(out_path)

        with _open_storage(out_path, metadata_format, mode="a") as f:
            base = _create_level(f, metadata_format, 0, src.shape, chunks, src.dtype)
            copy(src, output=base, block_shape=tuple(int(c) for c in base.chunks), **run_kwargs)
            _build_pyramid(f, base, src.shape, scale_factors, metadata_format, chunks,
                           src.dtype, order, anti_aliasing, run_kwargs)

    metadata_dict = {"resolution": list(resolution), "unit": unit, "setup_name": source_name}
    write_format_metadata(metadata_format, out_path, metadata_dict, scale_factors)


def compute_max_id(path, key, tmp_folder, target, max_jobs):
    job_type, job_config, num_workers = get_run_config(target, max_jobs, tmp_folder)
    with _open_data(path, mode="r") as f:
        src = as_source(f[key])
        max_id = stats.max(src, num_workers=num_workers, job_type=job_type, job_config=job_config)
    return int(max_id)


def add_max_id(in_path, in_key, out_path, out_key,
               tmp_folder, target, max_jobs):
    with _open_data(out_path, mode="r") as f_out:
        if "maxId" in f_out[out_key].attrs:
            return

    with open_file(in_path, "r") as f:
        max_id = f[in_key].attrs.get("maxId", None)

    if max_id is None:
        max_id = compute_max_id(out_path, out_key, tmp_folder, target, max_jobs)

    with _open_data(out_path, mode="a") as f:
        f[out_key].attrs["maxId"] = int(max_id)
