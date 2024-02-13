import os
from glob import glob

import numpy as np
import elf.skeleton.io as skio
from elf.io import open_file, is_h5py
from skimage.draw import disk
from pybdv.converter import make_scales
from pybdv.metadata import (write_h5_metadata,
                            write_n5_metadata,
                            write_xml_metadata)
from pybdv.util import get_key
from tqdm import tqdm


def is_ome_zarr(path):
    return path.endswith("ome.zarr")


def get_key_ome_zarr(path):
    with open_file(path, "r") as f:
        key = f.attrs["multiscales"][0]["datasets"][0]["path"]
    return key


def coords_to_vol(coords, nid, radius=5):
    bb_min = coords.min(axis=0)
    bb_max = coords.max(axis=0) + 1

    sub_shape = tuple(bma - bmi for bmi, bma in zip(bb_min, bb_max))
    sub_vol = np.zeros(sub_shape, dtype='int16')
    sub_coords = coords - bb_min

    xy_shape = sub_vol.shape[1:]
    for c in sub_coords:
        z, y, x = c
        mask = disk((y, x), radius, shape=xy_shape)
        sub_vol[z][mask] = nid

    return sub_vol


def vals_to_coords(vals, res):
    coords = np.array(vals)
    coords /= res
    coords = coords.astype('uint64')
    return coords


def parse_traces_from_nmx(trace_folder):
    """Extract all traced neurons stored in nmx format and return as dict.
    """
    trace_files = glob(os.path.join(trace_folder, "*.nmx"))
    if not trace_files:
        raise ValueError("Did not find any traces in %s" % trace_folder)
    coords = {}
    for path in trace_files:
        skel = skio.read_nml(path)
        search_str = 'neuron_id'
        for k, v in skel.items():
            # for now, we only extract nodes belonging to
            # what's annotated as 'skeleton'. There are also tags for
            # 'soma' and 'synapse'. I am ignoring these for now.

            # is_soma = 'soma' in k
            # is_synapse = 'synapse' in k
            is_skeleton = 'skeleton' in k
            if not is_skeleton:
                continue

            sub = k.find(search_str)
            beg = sub + len(search_str)
            end = k.find('.', beg)
            n_id = int(k[beg:end])

            # make sure we keep the order of keys when extracting the
            # values
            kvs = v.keys()
            c = [vv for kv in sorted(kvs) for vv in v[kv]]
            if n_id in coords:
                coords[n_id].extend(c)
            else:
                coords[n_id] = c
    return coords


# TODO enable passing the 'parse_id' function
def parse_traces_from_swc(trace_folder, parse_id=None):
    trace_files = glob(os.path.join(trace_folder, "*.swc"))
    if not trace_files:
        raise ValueError("Did not find any traces in %s" % trace_folder)
    coords = {}
    for n_id, path in enumerate(trace_files, 1):
        if parse_id is not None:
            n_id = parse_id(path)
        _, skel_coords, _ = skio.read_swc(path)
        coords[n_id] = skel_coords
    return coords


def parse_traces(trace_folder):
    have_nmx = len(glob(os.path.join(trace_folder, "*.nmx"))) > 0
    have_swc = len(glob(os.path.join(trace_folder, "*.swc"))) > 0

    if have_nmx and have_swc:
        raise ValueError(f"Found a mix of swc and nmx traces in {trace_folder}")

    if (not have_nmx) and (not have_swc):
        raise ValueError(f"Did not find any traces in {trace_folder}")

    if have_nmx:
        return parse_traces_from_nmx(trace_folder)
    else:
        return parse_traces_from_swc(trace_folder)


def traces_to_volume(traces, out_path, key, shape, resolution, chunks,
                     radius, n_threads, crop_overhanging=True):
    # write temporary h5 dataset
    # and write coordinates (with some radius) to it
    with open_file(out_path) as f:
        ds = f.require_dataset(key, shape=shape, dtype='int16', compression='gzip',
                               chunks=chunks)
        ds.n_threads = n_threads
        for nid, vals in tqdm(traces.items()):
            coords = vals_to_coords(vals, resolution)
            bb_min = coords.min(axis=0)
            bb_max = coords.max(axis=0) + 1
            assert all(bmi < bma for bmi, bma in zip(bb_min, bb_max))
            this_trace = coords_to_vol(coords, nid, radius=radius)

            if any(b > sh for b, sh in zip(bb_max, shape)):
                if crop_overhanging:
                    crop = [max(int(b - sh), 0) for b, sh in zip(bb_max, shape)]
                    print("Cropping by", crop)
                    vol_bb = tuple(slice(0, sh - cr)
                                   for sh, cr in zip(this_trace.shape, crop))
                    this_trace = this_trace[vol_bb]
                    bb_max = [b - crp for b, crp in zip(bb_max, crop)]
                else:
                    raise RuntimeError("Invalid bounding box: %s, %s" % (str(bb_max),
                                                                         str(shape)))

            bb = tuple(slice(int(bmi), int(bma)) for bmi, bma in zip(bb_min, bb_max))

            sub_vol = ds[bb]
            trace_mask = this_trace != 0
            sub_vol[trace_mask] = this_trace[trace_mask]
            ds[bb] = sub_vol


def import_traces(input_folder, out_path,
                  reference_path, reference_scale,
                  resolution, scale_factors,
                  radius=2, chunks=None, max_jobs=8,
                  unit='micrometer', source_name=None):
    """ Import trace data into the mobie format.

    input_folder [str] - folder with traces to be imported.
    out_path [str] - where to save the segmentation
    reference_path [str] - path to the reference volume
    reference_scale [str] - scale to use for reference
    resolution [list[float]] - resolution of the traces in micrometers
    scale_factors [list[list[int]]] - scale factors for down-sampling
    radius [int] - radius to write for the traces
    chunks [list[int]] - chunks for the traces volume
    max_jobs [int] - number of threads to use for down-samling
    unit [str] - physical unit (default: micrometer)
    source_name [str] - name of the source (default: None)
    """

    traces = parse_traces(input_folder)

    # check that we are compatible with bdv (ids need to be smaller than int16 max)
    max_id = np.iinfo("int16").max
    max_trace_id = max(traces.keys())
    if max_trace_id > max_id:
        raise RuntimeError("Can't export id %i > %i" % (max_trace_id, max_id))

    if is_ome_zarr(reference_path):
        ref_key = get_key_ome_zarr(reference_path)
    else:
        is_h5 = is_h5py(reference_path)
        ref_key = get_key(is_h5, timepoint=0, setup_id=0, scale=reference_scale)

    with open_file(reference_path, "r") as f:
        ds = f[ref_key]
        shape = ds.shape
        if chunks is None:
            chunks = ds.chunks

    key0 = get_key(is_h5, timepoint=0, setup_id=0, scale=0)
    print("Writing traces ...")
    traces_to_volume(traces, out_path, key0, shape, resolution, chunks, radius, max_jobs)

    print("Downscaling traces ...")
    make_scales(out_path, scale_factors, downscale_mode='max',
                ndim=3, setup_id=0, is_h5=is_h5,
                chunks=chunks, n_threads=max_jobs)

    xml_path = os.path.splitext(out_path)[0] + '.xml'
    # we assume that the resolution is in nanometer, but want to write in microns for bdv
    bdv_res = [res / 1000. for res in resolution]
    write_xml_metadata(xml_path, out_path, unit, bdv_res, is_h5,
                       setup_id=0, timepoint=0, setup_name=source_name,
                       affine=None, attributes={'channel': {'id': 0}},
                       overwrite=False, overwrite_data=False, enforce_consistency=False)
    bdv_scale_factors = [[1, 1, 1]] + scale_factors
    if is_h5:
        write_h5_metadata(out_path, bdv_scale_factors)
    else:
        write_n5_metadata(out_path, bdv_scale_factors, bdv_res)
