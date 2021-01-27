import argparse
from mobie.verification import verify_s3_dataset


def verify_platy_dataset(version, dataset_name, scale):
    server = 'https://s3.embl.de'
    bucket = 'platybrowser'

    path_in_bucket = f'platybrowser/{version}/{dataset_name}.n5'
    dataset_name = f'setup0/timepoint0/s{scale}'

    # TODO catch error if dataset / version / scale combination is invalid
    corrupted_chunks = verify_s3_dataset(bucket, path_in_bucket, dataset_name,
                                         server=server, anon=True)
    if corrupted_chunks:
        print("Found", len(corrupted_chunks), "corrupted chunks:")
        print(corrupted_chunks)
    else:
        print("None of the chunks are corrupted")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default='rawdata')
    parser.add_argument('--dataset_name', default='sbem-6dpf-1-whole-raw')
    parser.add_argument('--scale', default=9, type=int)

    args = parser.parse_args()
    verify_platy_dataset(args.version, args.dataset_name, args.scale)
