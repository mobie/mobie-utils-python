"""Validation functionality for data associate with MoBIE sources.
"""
import json
import math
import os
import warnings
from concurrent import futures
from itertools import product
from shutil import rmtree
from subprocess import run
from typing import List, Optional

import zarr
import z5py
import s3fs
from botocore.exceptions import ClientError
from tqdm import tqdm


def validate_chunks_local(dataset, chunk_ids, n_threads):
    """@private

    Return the chunk-grid positions whose data cannot be read / decoded. `dataset` is an open
    z5py dataset, so this works uniformly across zarr v2 (NGFF v0.4), zarr v3 (v0.5) and sharded v3.
    """
    chunks, shape = dataset.chunks, dataset.shape

    def validate_chunk(chunk_id):
        bb = tuple(slice(cid * ch, min((cid + 1) * ch, sh))
                   for cid, ch, sh in zip(chunk_id, chunks, shape))
        try:
            dataset[bb]
            return None
        except Exception:
            return chunk_id

    with futures.ThreadPoolExecutor(n_threads) as tp:
        corrupted_chunks = list(tqdm(tp.map(validate_chunk, chunk_ids), total=len(chunk_ids)))
    return [chunk_id for chunk_id in corrupted_chunks if chunk_id is not None]


def validate_local_dataset(
    path: str,
    dataset_name: str,
    n_threads: int,
    keys: Optional[List[tuple]] = None,
) -> List[tuple]:
    """Validate the chunks in a locally stored zarr array.

    Reads the data via z5py, which handles the zarr v2 (NGFF v0.4), zarr v3 (v0.5) and sharded v3
    layouts uniformly.

    Args:
        path: The path to the zarr root group.
        dataset_name: The internal name of the zarr dataset / array.
        n_threads: The number of threads to use for computation.
        keys: Optional list of chunk-grid positions (tuples) to be checked. This can for example be
            used to re-check positions that were previously identified as corrupted.

    Returns:
        The list of corrupted chunk-grid positions in the zarr array.
    """
    with z5py.File(path, "r") as f:
        ds = f[dataset_name]
        if keys is None:
            chunks, shape = ds.chunks, ds.shape
            grid = [range(int(math.ceil(sh / ch))) for sh, ch in zip(shape, chunks)]
            keys = list(product(*grid))
        return validate_chunks_local(ds, keys, n_threads)


def validate_chunks_s3(store, dataset, keys, n_threads, max_tries=5):
    """@private
    """
    max_tries = 4

    def load_from_store(key, n_tries):
        while n_tries < max_tries:
            try:
                cdata = store[key]
                return cdata
            except ClientError:
                load_from_store(key, n_tries+1)

    def validate_chunk(key):
        cdata = load_from_store(key, n_tries=0)
        try:
            cdata = dataset._decode_chunk(cdata)
        except Exception:
            return key

    with futures.ThreadPoolExecutor(n_threads) as tp:
        corrupted_chunks = list(tqdm(tp.map(validate_chunk, keys)))
    corrupted_chunks = [key for key in corrupted_chunks if key is not None]

    # drop the non-chunk metadata sentinels: n5 / zarr v2 use 'attributes.json', zarr v3 'zarr.json'.
    for sentinel in ("attributes.json", "zarr.json", ".zarray", ".zattrs", ".zgroup"):
        if sentinel in corrupted_chunks:
            corrupted_chunks.remove(sentinel)

    return corrupted_chunks


def _get_fs(server, anon):
    client_kwargs = {}
    if server is not None:
        client_kwargs.update({"endpoint_url": server})
    fs = s3fs.S3FileSystem(anon=anon, client_kwargs=client_kwargs)
    return fs


# zarr doesn't support s3 n5 yet, so we need to hack it...
def validate_s3_dataset(
    bucket_name: str,
    path_in_bucket: str,
    dataset_name: str,
    server: Optional[str] = None,
    anon: bool = True,
    n_threads: int = 1,
) -> List[str]:
    """Validate the chunks in a zarr array stored on s3.

    Args:
        bucket_name: The name of the s3 bucket.
        path_in_bucket: The path in the bucket to the zarr root group.
        dataset_name: The internal name of the zarr dataset / array.
        server: Optional server endpoint url.
        anon: Whether to use anonymous access in the s3 client.
        n_threads: The number of threads to use for computation.

    Returns:
        The list of corrupted chunks in the zarr array.
    """

    tmp_file = "./tmp_file.n5"
    os.makedirs(tmp_file, exist_ok=True)

    fs = _get_fs(server, anon)
    # make a dummy local file by copying the relevant attributes.json
    store = s3fs.S3Map(root=path_in_bucket, s3=fs)
    attrs = store["attributes.json"].decode("utf-8")
    attrs = json.loads(attrs)
    attrs_file = os.path.join(tmp_file, "attributes.json")
    with open(attrs_file, "w") as f:
        json.dump(attrs, f)

    # make a dummy dataset by copying the dataset attributes.json
    store = s3fs.S3Map(root=os.path.join(path_in_bucket, dataset_name), s3=fs)
    try:
        attrs = store["attributes.json"].decode("utf-8")
    except KeyError:
        try:
            rmtree(tmp_file)
        except OSError:
            pass
        raise ValueError(f"No file {path_in_bucket}:{dataset_name} in {bucket_name}")

    attrs = json.loads(attrs)
    tmp_ds = os.path.join(tmp_file, dataset_name)
    os.makedirs(tmp_ds, exist_ok=True)
    attrs_file = os.path.join(tmp_ds, "attributes.json")
    with open(attrs_file, "w") as f:
        json.dump(attrs, f)

    print("validating chunks for s3 dataset stored at")
    if server is None:
        print(f"{bucket_name}:{path_in_bucket}:{dataset_name}")
    else:
        print(f"{server}:{bucket_name}:{path_in_bucket}:{dataset_name}")
    dataset = zarr.open(tmp_file)[dataset_name]
    corrupted_chunks = validate_chunks_s3(store, dataset, keys=iter(store), n_threads=n_threads)

    try:
        rmtree(tmp_file)
    except OSError:
        warnings.warn(f"Could not clean up temporary data stored in {tmp_file}")
        pass

    return corrupted_chunks


# then non-anon authentication doesn"t work for the embl s3 server
# so we can"t use "fix_corrupted_chunks_s3", hence use the minioclient
def fix_corrupted_chunks_minio(corrupted_chunks,
                               local_dataset_path,
                               local_dataset_key,
                               bucket_name,
                               path_in_bucket,
                               dataset_name,
                               server="embl"):
    """@private
    """
    try:
        local_ds = zarr.open(local_dataset_path, "r")[local_dataset_key]
    except KeyError:
        raise ValueError(f"No file {path_in_bucket}:{dataset_name} in {bucket_name}")

    local_corrupted_chunks = []
    for chunk_id in corrupted_chunks:
        local_chunk_path = os.path.join(local_dataset_path, local_dataset_key, chunk_id)
        with open(local_chunk_path, "rb") as f:
            cdata = f.read()
        try:
            cdata = local_ds._decode_chunk(cdata)
        except Exception:
            local_corrupted_chunks.append(chunk_id)
        remote_chunk_path = os.path.join("embl", bucket_name, path_in_bucket, dataset_name, chunk_id)
        mc_command = ["mc", "cp", local_chunk_path, remote_chunk_path]
        run(mc_command)

    return local_corrupted_chunks


def fix_corrupted_chunks_s3(corrupted_chunks,
                            local_dataset_path,
                            local_dataset_key,
                            bucket_name,
                            path_in_bucket,
                            dataset_name,
                            server=None,
                            anon=False):
    """@private
    """
    try:
        local_ds = zarr.open(local_dataset_path, "r")[local_dataset_key]
    except KeyError:
        raise ValueError(f"No file {path_in_bucket}:{dataset_name} in {bucket_name}")

    fs = _get_fs(server, anon)
    store = s3fs.S3Map(root=os.path.join(path_in_bucket, dataset_name), s3=fs)

    local_corrupted_chunks = []
    for chunk_id in corrupted_chunks:
        local_chunk_path = os.path.join(local_dataset_path, local_dataset_key, chunk_id)
        with open(local_chunk_path, "rb") as f:
            cdata = f.read()
        try:
            cdata = local_ds._decode_chunk(cdata)
        except Exception:
            local_corrupted_chunks.append(chunk_id)
        store[chunk_id] = cdata

    return local_corrupted_chunks
