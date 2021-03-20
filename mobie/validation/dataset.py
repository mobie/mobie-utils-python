import argparse
import os
import json
from glob import glob

from .utils import _assert_true, _assert_in, validate_with_schema
from .metadata import validate_source_metadata, validate_view_metadata


def validate_dataset(dataset_folder, assert_true=_assert_true, assert_in=_assert_in):
    # check the source metadata
    source_metadata_path = os.path.join(dataset_folder, "dataset.json")
    msg = f"Cannot find metadata file at {source_metadata_path}"
    assert_true(os.path.exists(source_metadata_path), msg)
    with open(source_metadata_path) as f:
        dataset_metadata = json.load(f)

    # static validation
    validate_with_schema(dataset_metadata, "dataset")

    # check the sources
    for name, metadata in dataset_metadata["sources"].items():
        validate_source_metadata(name, metadata, dataset_folder,
                                 assert_true=assert_true)

    # check the bookmarks
    bookmark_folder = os.path.join(dataset_folder, 'misc', 'bookmarks')
    all_sources = list(dataset_metadata["sources"].keys())
    bookmark_files = glob(os.path.join(bookmark_folder, '*.json'))
    for bookmark_file in bookmark_files:
        with open(bookmark_file, 'r') as f:
            bookmarks = json.load(f)
        for name, bookmark in bookmarks.items():
            validate_view_metadata(bookmark, all_sources, assert_true)


def main():
    parser = argparse.ArgumentParser("Validate MoBIE dataset metadata")
    parser.add_argument('--input', '-i', type=str, required=True,
                        help="the dataset location")
    args = parser.parse_args()
    validate_dataset(args.input)
