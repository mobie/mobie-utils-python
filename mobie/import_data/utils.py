import json
import os
import numpy as np

import luigi
import nifty.distributed as ndist

from cluster_tools.statistics import DataStatisticsWorkflow
from cluster_tools.downscaling import DownscalingWorkflow
from cluster_tools.node_labels import NodeLabelWorkflow
from elf.io import open_file
from ..utils import write_global_config


def compute_node_labels(seg_path, seg_key,
                        input_path, input_key,
                        tmp_folder, target, max_jobs,
                        prefix="", ignore_label=None, max_overlap=True):
    task = NodeLabelWorkflow
    config_folder = os.path.join(tmp_folder, "configs")

    out_path = os.path.join(tmp_folder, "data.n5")
    out_key = "node_labels_%s" % prefix

    t = task(tmp_folder=tmp_folder, config_dir=config_folder,
             max_jobs=max_jobs, target=target,
             ws_path=seg_path, ws_key=seg_key,
             input_path=input_path, input_key=input_key,
             output_path=out_path, output_key=out_key,
             prefix=prefix, max_overlap=max_overlap,
             ignore_label=ignore_label)
    ret = luigi.build([t], local_scheduler=True)
    if not ret:
        raise RuntimeError("Node label computation for %s failed" % prefix)

    f = open_file(out_path, "r")
    ds_out = f[out_key]

    if max_overlap:
        data = ds_out[:]
    else:
        n_chunks = ds_out.number_of_chunks
        data = [ndist.deserializeOverlapChunk(out_path, out_key, (chunk_id,))[0]
                for chunk_id in range(n_chunks)]
        data = {label_id: overlaps
                for chunk_data in data
                for label_id, overlaps in chunk_data.items()}
    return data


def check_input_data(in_path, in_key, resolution, require3d, channel, roi_begin=None, roi_end=None):
    # TODO to support data with channel, we need to support downscaling with channels
    if channel is not None:
        raise NotImplementedError
    with open_file(in_path, "r") as f:
        ndim = f[in_key].ndim

    if require3d and ndim != 3:
        raise ValueError(f"Expect 3d data, got ndim={ndim}")
    if len(resolution) != ndim:
        raise ValueError(f"Expect same length of resolution as ndim, got: resolution={resolution}, ndim={ndim}")


def downscale(in_path, in_key, out_path,
              resolution, scale_factors, chunks,
              tmp_folder, target, max_jobs, block_shape,
              library="vigra", library_kwargs=None,
              metadata_format="ome.zarr", out_key="",
              unit="micrometer", source_name=None,
              roi_begin=None, roi_end=None, fit_to_roi=False,
              int_to_uint=False, channel=None):
    task = DownscalingWorkflow

    block_shape = chunks if block_shape is None else block_shape
    config_dir = os.path.join(tmp_folder, "configs")
    # ome.zarr can also be written in 2d, all other formats require 3d
    require3d = metadata_format != "ome.zarr"
    check_input_data(in_path, in_key, resolution, require3d, channel, roi_begin=roi_begin, roi_end=roi_end)
    write_global_config(config_dir, block_shape=block_shape, require3d=require3d,
                        roi_begin=roi_begin, roi_end=roi_end, fit_to_roi=fit_to_roi)

    configs = DownscalingWorkflow.get_config()
    conf = configs["copy_volume"]
    conf.update({"chunks": chunks, "time_limit": 600})
    with open(os.path.join(config_dir, "copy_volume.config"), "w") as f:
        json.dump(conf, f)

    ds_conf = configs["downscaling"]
    ds_conf.update({"chunks": chunks, "library": library, "time_limit": 600})
    if library_kwargs is not None:
        ds_conf.update({"library_kwargs": library_kwargs})
    with open(os.path.join(config_dir, "downscaling.config"), "w") as f:
        json.dump(ds_conf, f)

    halos = scale_factors
    metadata_dict = {"resolution": resolution, "unit": unit, "setup_name": source_name}

    t = task(tmp_folder=tmp_folder, config_dir=config_dir,
             target=target, max_jobs=max_jobs,
             input_path=in_path, input_key=in_key,
             scale_factors=scale_factors, halos=halos,
             metadata_format=metadata_format, metadata_dict=metadata_dict,
             output_path=out_path, output_key_prefix=out_key,
             int_to_uint=int_to_uint)
    ret = luigi.build([t], local_scheduler=True)
    if not ret:
        raise RuntimeError("Downscaling failed")


def compute_max_id(path, key, tmp_folder, target, max_jobs):
    # the workflow only works for 3d data, and 2d data is usually small enough to easily do this in memory
    with open_file(path, "r") as f:
        ds = f[key]
        if ds.ndim == 2:
            ds.n_threads = max_jobs
            max_id = int(ds[:].max())
            return max_id

    task = DataStatisticsWorkflow
    stat_path = os.path.join(tmp_folder, "statistics.json")
    t = task(tmp_folder=tmp_folder, config_dir=os.path.join(tmp_folder, "configs"),
             target=target, max_jobs=max_jobs,
             path=path, key=key, output_path=stat_path)
    ret = luigi.build([t], local_scheduler=True)
    if not ret:
        raise RuntimeError("Computing max id failed")

    with open(stat_path) as f:
        stats = json.load(f)

    return stats["max"]


def add_max_id(in_path, in_key, out_path, out_key,
               tmp_folder, target, max_jobs):
    with open_file(out_path, "r") as f_out:
        ds_out = f_out[out_key]
        if "maxId" in ds_out.attrs:
            return

    with open_file(in_path, "r") as f:
        max_id = f[in_key].attrs.get("maxId", None)

    if max_id is None:
        max_id = compute_max_id(out_path, out_key, tmp_folder, target, max_jobs)

    with open_file(out_path, "a") as f:
        f[out_key].attrs["maxId"] = int(max_id)


def ensure_volume(in_path, in_key, tmp_folder, chunks):
    with open_file(in_path, mode="r") as f:
        ndim = len(f[in_key].shape)
    if ndim not in (2, 3):
        raise ValueError(f"Expected input of dimension 2 or 3, got {ndim}")

    if ndim == 2:
        assert chunks[0] == 1, f"{chunks}"
        with open_file(in_path, mode="r") as f:
            ds = f[in_key]
            img = ds[:]

        name = os.path.splitext(os.path.split(in_path)[1])[0]
        tmp_path = os.path.join(tmp_folder, f"tmp_{name}.h5")
        tmp_key = "data"

        os.makedirs(tmp_folder, exist_ok=True)
        with open_file(tmp_path, mode="a") as f:
            f.create_dataset(tmp_key, data=img[None], chunks=tuple(chunks))
        return tmp_path, tmp_key
    else:
        return in_path, in_key


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
