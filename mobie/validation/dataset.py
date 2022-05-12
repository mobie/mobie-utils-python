import argparse
import os
import json
from glob import glob

from .utils import _assert_equal, _assert_true, _assert_in, validate_with_schema
from .metadata import validate_source_metadata, validate_view_metadata


def validate_dataset(dataset_folder, require_data=True,
                     assert_true=_assert_true, assert_in=_assert_in, assert_equal=_assert_equal):
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
        validate_source_metadata(
            name, metadata, dataset_folder, require_data=require_data,
            assert_true=assert_true, assert_equal=assert_equal
        )

    # check the sources
    views_folder = os.path.join(dataset_folder, "misc", "views")
    all_sources = list(dataset_metadata["sources"].keys())
    view_files = glob(os.path.join(views_folder, "*.json"))
    for view_file in view_files:
        with open(view_file, "r") as f:
            views = json.load(f)["views"]
        for name, view in views.items():
            validate_view_metadata(
                view, sources=all_sources, dataset_folder=dataset_folder, assert_true=assert_true
            )


def main():
    parser = argparse.ArgumentParser("Validate MoBIE dataset metadata")
    parser.add_argument("--input", "-i", type=str, required=True, help="the dataset location")
    parser.add_argument("--require_data", "-r", type=int, default=1, help="whether to require that local data exists")
    args = parser.parse_args()
    validate_dataset(args.input, require_data=bool(args.require_data))
