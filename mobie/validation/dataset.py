import argparse
import os
import json
from glob import glob

from .utils import _assert_true, _assert_in
from .metadata import validate_source_metadata, validate_view_metadata


def validate_dataset(dataset_folder, assert_true=_assert_true, assert_in=_assert_in):

    # check the source metadata
    source_metadata_path = os.path.join(dataset_folder, "sources.json")
    msg = f"Cannot find metadata file at {source_metadata_path}"
    assert_true(os.path.exists(source_metadata_path), msg)

    with open(source_metadata_path) as f:
        sources_metadata = json.load(f)
    for name, metadata in sources_metadata.items():
        validate_source_metadata(name, metadata, dataset_folder,
                                 assert_true=assert_true)

    # check the bookmarks
    bookmark_folder = os.path.join(dataset_folder, 'misc', 'bookmarks')
    msg = f"Cannot find bookmark folder at {bookmark_folder}"
    assert_true(os.path.isdir(bookmark_folder), msg)
    default_bookmark = os.path.join(bookmark_folder, 'default.json')
    msg = f"Cannot find default bookmark file at {default_bookmark}"
    assert_true(os.path.exists(default_bookmark), msg)

    all_sources = list(sources_metadata.keys())
    bookmark_files = glob(os.path.join(bookmark_folder, '*.json'))
    for bookmark_file in bookmark_files:
        with open(bookmark_file, 'r') as f:
            bookmarks = json.load(f)
        if os.path.basename(bookmark_file) == 'default.json':
            msg = f"Cannot find 'default' bookmark in {bookmark_file}"
            assert_in('default', bookmarks, msg)
        for name, bookmark in bookmarks.items():
            validate_view_metadata(bookmark, all_sources, assert_true)


def main():
    parser = argparse.ArgumentParser("Validate MoBIE dataset metadata")
    parser.add_argument('--input', '-i', type=str, required=True,
                        help="the dataset location")
    args = parser.parse_args()
    validate_dataset(args.input)
