"""Validation functionality for a MoBIE dataset.
"""
import argparse
import os
import json
from glob import glob
from typing import Callable

from tqdm import tqdm
from .utils import _assert_equal, _assert_true, _assert_in, validate_with_schema
from .metadata import validate_source_metadata, validate_view_metadata


def validate_dataset(
    dataset_folder: str,
    require_local_data: bool = True,
    require_remote_data: bool = False,
    assert_true: Callable = _assert_true,
    assert_in: Callable = _assert_in,
    assert_equal: Callable = _assert_equal,
    suppress_warnings: bool = False,
) -> None:
    """Validate that a MoBIE dataset adheres to the specification.

    Raises a ValueError if the dataset does not adhere to the spec.
    The type of error that is thrown can be modified by over-writing
    the assert_true, assert_in, and assert_equal arguments.

    Args:
        dataset_folder: The folder to the dataset.
        require_loca_data: Whether to require that local source data,
            i.e. the corresponding ome.zarr or bdv files, exists.
        require_remote_data: Whether to require that remote source data,
            i.e. the corresponding ome.zarr or bdv.n5.s3 file exists.
        assert_true: Function to over-write the default assert_true check.
        assert_in: Function to over-write the default assert_in check.
        assert_equal: Function to over-write the default assert_equal check.
        suppress_warnings: Whether to suppress valdiation warnings.
    """

    # check the source metadata
    ds_metadata_path = os.path.join(dataset_folder, "dataset.json")
    assert_true(os.path.exists(ds_metadata_path), f"Cannot find file {ds_metadata_path}")
    with open(ds_metadata_path) as f:
        dataset_metadata = json.load(f)

    # static validation
    validate_with_schema(dataset_metadata, "dataset")

    # check the sources
    ds_name = os.path.split(dataset_folder)[1]
    is_2d = dataset_metadata.get("is2D", False)
    for name, metadata in tqdm(
        dataset_metadata["sources"].items(),
        total=len(dataset_metadata["sources"]),
        desc=f"Check sources for dataset {ds_name}"
    ):
        validate_source_metadata(
            name, metadata,
            dataset_folder=dataset_folder, is_2d=is_2d,
            require_local_data=require_local_data,
            require_remote_data=require_remote_data,
            assert_true=assert_true,
            assert_equal=assert_equal,
            assert_in=assert_in,
            suppress_warnings=suppress_warnings,
        )

    # check the views
    all_sources = list(dataset_metadata["sources"].keys())
    for name, view in tqdm(
        dataset_metadata["views"].items(),
        total=len(dataset_metadata["views"]),
        desc=f"Check views for dataset {ds_name}"
    ):
        validate_view_metadata(
            view, sources=all_sources, dataset_folder=dataset_folder, assert_true=assert_true,
            dataset_metadata=dataset_metadata
        )

    # check the (potential) additional view files
    views_folder = os.path.join(dataset_folder, "misc", "views")
    view_files = glob(os.path.join(views_folder, "*.json"))
    for view_file in tqdm(view_files, desc=f"Check view files for dataset {ds_name}"):
        with open(view_file, "r") as f:
            views = json.load(f)["views"]
        for name, view in views.items():
            validate_view_metadata(
                view, sources=all_sources, dataset_folder=dataset_folder, assert_true=assert_true,
                dataset_metadata=dataset_metadata
            )


def main():
    """@private
    """
    parser = argparse.ArgumentParser("Validate MoBIE dataset metadata")
    parser.add_argument("--input", "-i", type=str, required=True, help="the dataset location")
    parser.add_argument("--require_local_data", "-r", type=int, default=1, help="check that local data exists")
    parser.add_argument("--require_remote_data", "-d", type=int, default=0, help="check that remote data exists")
    args = parser.parse_args()
    validate_dataset(
        args.input, require_local_data=bool(args.require_local_data), require_remote_data=bool(args.require_remote_data)
    )
