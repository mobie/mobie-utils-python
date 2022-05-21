import argparse
import os
import json
from glob import glob

from tqdm import tqdm
from .utils import _assert_equal, _assert_true, _assert_in, validate_with_schema
from .metadata import validate_source_metadata, validate_view_metadata


def validate_dataset(dataset_folder, require_local_data=True, require_remote_data=False,
                     assert_true=_assert_true, assert_in=_assert_in, assert_equal=_assert_equal):

    # check the source metadata
    ds_metadata_path = os.path.join(dataset_folder, "dataset.json")
    assert_true(os.path.exists(ds_metadata_path), f"Cannot find file {ds_metadata_path}")
    with open(ds_metadata_path) as f:
        dataset_metadata = json.load(f)

    # static validation
    validate_with_schema(dataset_metadata, "dataset")

    # check the sources
    ds_name = os.path.split(dataset_folder)[1]
    for name, metadata in tqdm(
        dataset_metadata["sources"].items(),
        total=len(dataset_metadata["sources"]),
        desc=f"Check sources for dataset {ds_name}"
    ):
        validate_source_metadata(
            name, metadata, dataset_folder,
            require_local_data=require_local_data,
            require_remote_data=require_remote_data,
            assert_true=assert_true, assert_equal=assert_equal
        )

    # check the views
    all_sources = list(dataset_metadata["sources"].keys())
    for name, view in tqdm(
        dataset_metadata["views"].items(),
        total=len(dataset_metadata["views"]),
        desc=f"Check views for dataset {ds_name}"
    ):
        validate_view_metadata(
            view, sources=all_sources, dataset_folder=dataset_folder, assert_true=assert_true
        )

    # check the (potential) additional view files
    views_folder = os.path.join(dataset_folder, "misc", "views")
    view_files = glob(os.path.join(views_folder, "*.json"))
    for view_file in tqdm(view_files, desc=f"Check view files for dataset {ds_name}"):
        with open(view_file, "r") as f:
            views = json.load(f)["views"]
        for name, view in views.items():
            validate_view_metadata(
                view, sources=all_sources, dataset_folder=dataset_folder, assert_true=assert_true
            )


def main():
    parser = argparse.ArgumentParser("Validate MoBIE dataset metadata")
    parser.add_argument("--input", "-i", type=str, required=True, help="the dataset location")
    parser.add_argument("--require_local_data", "-r", type=int, default=1, help="check that local data exists")
    parser.add_argument("--require_remote_data", "-d", type=int, default=0, help="check that remote data exists")
    args = parser.parse_args()
    validate_dataset(
        args.input, require_local_data=bool(args.require_local_data), require_remote_data=bool(args.require_remote_data)
    )
