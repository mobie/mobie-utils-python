import json
import os
import shutil
from glob import glob

from mobie.verification import verify_s3_dataset, fix_corrupted_chunks_minio
from mobie.xml_utils import read_path_in_bucket
from pybdv.metadata import get_data_path

SAVE_DIR = './corrupted_chunks'


# TODO move this to mobie.verification.py

def verify_dataset(dataset_key, scale, n_threads):
    server = 'https://s3.embl.de'
    bucket = 'platybrowser'

    path_in_bucket = f'platybrowser/{dataset_key}'
    dataset_name = f'setup0/timepoint0/s{scale}'

    corrupted_chunks = verify_s3_dataset(bucket, path_in_bucket, dataset_name,
                                         server=server, anon=True)
    return corrupted_chunks


def verify_source(source):
    source_name = os.path.split(source)[1]
    source_name = os.path.splitext(source_name)[0]
    source_key = read_path_in_bucket(source)
    n_threads = 8
    scale = 0
    while True:
        print("Verifing scale", scale, "of source", source)
        try:
            corrupted_chunks = verify_dataset(source_key, scale=scale, n_threads=n_threads)
        # scale level doesn't exist anymore
        except ValueError:
            break

        if corrupted_chunks:  # did we find corrupted chunks?
            save_path = f'corrupted_chunks/corrupted_chunks_{source_name}_s{scale}.json'
            print("Found", len(corrupted_chunks), "corrupted_chunks. Saving corrupted_chunk ids to", save_path)
            with open(save_path, 'w') as f:
                json.dump(corrupted_chunks, f)

        scale += 1


def verify_all():
    pattern = '/g/kreshuk/pape/Work/mobie/platybrowser-datasets/data/1.0.1/images/remote/*.xml'
    xmls = glob(pattern)
    xmls.sort()

    checked_file = './verified_sources.json'
    if os.path.exists(checked_file):
        with open(checked_file, 'r') as f:
            checked_sources = json.load(f)
    else:
        checked_sources = []

    for source in xmls:
        source_name = os.path.split(source)[1]
        source_name = os.path.splitext(source_name)[0]

        if source_name in checked_sources:
            continue

        # do raw seperately because it's so massive
        if 'xray' in source_name:
            continue

        print("Verifying source:", source_name)
        verify_source(source)

        checked_sources.append(source_name)
        with open(checked_file, 'w') as f:
            json.dump(checked_sources, f)


def fix_chunks_dataset(source, scale, corrupted_chunks):
    bucket = 'platybrowser'

    source_s3_key = read_path_in_bucket(source)
    local_ds_path = get_data_path(source.replace('remote', 'local'), return_absolute_path=True)
    ds_key = f'setup0/timepoint0/s{scale}'

    return fix_corrupted_chunks_minio(corrupted_chunks,
                                      local_ds_path,
                                      ds_key,
                                      bucket,
                                      source_s3_key,
                                      ds_key,
                                      server='embl')


def fix_chunks_source(source):
    source_name = os.path.split(source)[1]
    source_name = os.path.splitext(source_name)[0]

    pattern = os.path.join(f'./corrupted_chunks/corrupted_chunks_{source_name}_s*.json')
    corrupted_chunk_files = glob(pattern)
    for corrupted_path in corrupted_chunk_files:

        scale = corrupted_path.split('_')[-1]
        scale = scale.split('.')[0]
        scale = int(scale[1:])

        with open(corrupted_path, 'r') as f:
            corrupted_chunks = json.load(f)
        print("Fixing", len(corrupted_chunks), "corrupted chunk for", source_name, "at scale", scale)

        local_corrupted = fix_chunks_dataset(source, scale=scale,
                                             corrupted_chunks=corrupted_chunks)

        if local_corrupted:
            print(len(local_corrupted), "/", len(corrupted_chunks), "chunks are also corrupted locally")
            local_corrupted_path = f'./corrupted_chunks/local_corrupted_chunks_{source_name}_s*.json'
            print("Save chunk ids to", local_corrupted_path)
            with open(local_corrupted_path, 'w') as f:
                json.dump(local_corrupted, f)
            return False

        bkp_path = corrupted_path.replace('.json', '.bkp')
        shutil.move(corrupted_path, bkp_path)

    return True


def fix_all():
    pattern = '/g/kreshuk/pape/Work/mobie/platybrowser-datasets/data/1.0.1/images/remote/*.xml'
    xmls = glob(pattern)
    xmls.sort()

    fixed_file = './fixed_sources.json'
    if os.path.exists(fixed_file):
        with open(fixed_file, 'r') as f:
            fixed_sources = json.load(f)
    else:
        fixed_sources = []

    for source in xmls:
        source_name = os.path.split(source)[1]
        source_name = os.path.splitext(source_name)[0]

        if source_name in fixed_sources:
            continue

        chunks_fixed = fix_chunks_source(source)
        if not chunks_fixed:
            break

        fixed_sources.append(source_name)
        with open(fixed_file, 'w') as f:
            json.dump(fixed_sources, f)


if __name__ == '__main__':
    # source = os.path.join('/g/kreshuk/pape/Work/mobie/platybrowser-datasets/data/1.0.1/images/remote',
    #                      'sbem-6dpf-1-whole-segmented-shell.xml')
    # verify_source(source)
    # fix_chunks_source(source)

    verify_all()
    fix_all()
