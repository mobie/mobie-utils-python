import json
import os
import warnings
from concurrent import futures
from shutil import rmtree
from subprocess import run

import zarr
import s3fs
from botocore.exceptions import ClientError
from tqdm import tqdm


def validate_chunks_local(store, dataset, keys, n_threads):

    def validate_chunk(chunk_key):
        key = os.path.join(dataset.path, chunk_key)
        cdata = store[key]
        try:
            cdata = dataset._decode_chunk(cdata)
        except Exception:
            return chunk_key

    with futures.ThreadPoolExecutor(n_threads) as tp:
        corrupted_chunks = list(tqdm(tp.map(validate_chunk, keys)))
    corrupted_chunks = [key for key in corrupted_chunks if key is not None]
    return corrupted_chunks


def validate_local_dataset(path, dataset_name, n_threads, keys=None):
    f = zarr.open(path, mode="r")
    ds = f[dataset_name]
    store = ds.store
    if not store._is_array(ds.path):
        raise ValueError("Expected a dataset")

    if keys is None:
        keys = list(set(store.listdir(ds.path)) - set([".zarray", ".zattrs"]))
    return validate_chunks_local(store, ds, keys, n_threads)


def validate_chunks_s3(store, dataset, keys, n_threads, max_tries=5):
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

    if "attributes.json" in corrupted_chunks:
        corrupted_chunks.remove("attributes.json")

    return corrupted_chunks


def _get_fs(server, anon):
    client_kwargs = {}
    if server is not None:
        client_kwargs.update({"endpoint_url": server})
    fs = s3fs.S3FileSystem(anon=anon, client_kwargs=client_kwargs)
    return fs


# zarr doesn"t support s3 n5 yet, so we need to hack it...
def validate_s3_dataset(bucket_name,
                        path_in_bucket,
                        dataset_name,
                        server=None,
                        anon=True,
                        n_threads=1):

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
