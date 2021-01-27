import json
import os
from concurrent import futures
from shutil import rmtree

import zarr
import s3fs
from tqdm import tqdm


def verify_chunks_local(store, dataset, keys, n_threads):

    def verify_chunk(chunk_key):
        key = os.path.join(dataset.path, chunk_key)
        cdata = store[key]
        try:
            cdata = dataset._decode_chunk(cdata)
        except Exception:
            return chunk_key

    with futures.ThreadPoolExecutor(n_threads) as tp:
        corrupted_chunks = list(tqdm(tp.map(verify_chunk, keys)))
    corrupted_chunks = [key for key in corrupted_chunks if key is not None]
    return corrupted_chunks


def verify_local_dataset(path, dataset_name, n_threads):
    f = zarr.open(path, mode='r')
    ds = f[dataset_name]
    store = ds.store
    if not store._is_array(ds.path):
        raise ValueError("Expected a dataset")

    keys = list(set(store.listdir(ds.path)) - set(['.zarray', '.zattrs']))
    return verify_chunks_local(store, ds, keys, n_threads)


def verify_chunks_s3(store, dataset, keys, n_threads):
    def verify_chunk(key):
        cdata = store[key]
        try:
            cdata = dataset._decode_chunk(cdata)
        except Exception:
            return key

    with futures.ThreadPoolExecutor(n_threads) as tp:
        # corrupted_chunks = [verify_chunk(key) for key in keys]
        corrupted_chunks = list(tqdm(tp.map(verify_chunk, keys)))
    corrupted_chunks = [key for key in corrupted_chunks if key is not None]
    return corrupted_chunks


# zarr doesn't support s3 n5 yet, so we need to hack it...
def verify_s3_dataset(bucket_name,
                      path_in_bucket,
                      dataset_name,
                      server=None,
                      anon=True,
                      n_threads=1):

    if server is None:
        fs = s3fs.S3FileSystem(anon=anon)
    else:
        fs = s3fs.S3FileSystem(anon=anon,
                               client_kwargs={'endpoint_url': server})

    tmp_file = './tmp_file.n5'
    os.makedirs(tmp_file, exist_ok=True)

    # make a dummy local file by copying the relevant attributes.json
    store = s3fs.S3Map(root=path_in_bucket, s3=fs)
    attrs = store['attributes.json'].decode('utf-8')
    attrs = json.loads(attrs)
    attrs_file = os.path.join(tmp_file, 'attributes.json')
    with open(attrs_file, 'w') as f:
        json.dump(attrs, f)

    # make a dummy dataset by copying the dataset attributes.json
    store = s3fs.S3Map(root=os.path.join(path_in_bucket, dataset_name), s3=fs)
    attrs = store['attributes.json'].decode('utf-8')
    attrs = json.loads(attrs)
    tmp_ds = os.path.join(tmp_file, dataset_name)
    os.makedirs(tmp_ds, exist_ok=True)
    attrs_file = os.path.join(tmp_ds, 'attributes.json')
    with open(attrs_file, 'w') as f:
        json.dump(attrs, f)

    print("Verifying chunks for s3 dataset stored at")
    if server is None:
        print(f"{bucket_name}:{path_in_bucket}:{dataset_name}")
    else:
        print(f"{server}:{bucket_name}:{path_in_bucket}:{dataset_name}")
    dataset = zarr.open(tmp_file)[dataset_name]
    verify_chunks_s3(store, dataset, keys=iter(store), n_threads=n_threads)

    try:
        rmtree(tmp_file)
    except OSError:
        pass
