"""Validation functionality for a MoBIE project.
"""
import argparse
import json
import os
from typing import Callable

from .dataset import validate_dataset
from .utils import _assert_true, _assert_in, _assert_equal, validate_with_schema
from ..__version__ import SPEC_VERSION


def check_version(version_a, version_b, assert_equal):
    """@private
    """

    def parse_version(version):
        version_split = version.split(".")
        msg = f"Invalid version format {version}, expected 'MAJOR.MINOR.PATCH'"
        assert_equal(len(version_split), 3, msg)
        return version_split

    major_a, minor_a, patch_a = parse_version(version_a)
    major_b, minor_b, patch_b = parse_version(version_b)

    msg = f"Major versions do not match: {major_a}, {major_b}"
    assert_equal(major_a, major_b, msg)

    if major_a == 0:
        msg = f"Minor versions do no match: {minor_a}, {minor_b}"
        assert_equal(minor_a, minor_b, msg)


def validate_project(
    root: str,
    require_local_data: bool = True,
    require_remote_data: bool = False,
    assert_true: Callable = _assert_true,
    assert_in: Callable = _assert_in,
    assert_equal: Callable = _assert_equal,
) -> None:
    """Validate that a MoBIE project adheres to the specification.

    Raises a ValueError if the project does not adhere to the spec.
    The type of error that is thrown can be modified by over-writing
    the assert_true, assert_in, and assert_equal arguments.

    Args:
        root: The root directory of the MoBIE project.
        require_loca_data: Whether to require that local source data,
            i.e. the corresponding ome.zarr or bdv files, exists.
        require_remote_data: Whether to require that remote source data,
            i.e. the corresponding ome.zarr or bdv.n5.s3 file exists.
        assert_true: Function to over-write the default assert_true check.
        assert_in: Function to over-write the default assert_in check.
        assert_equal: Function to over-write the default assert_equal check.
    """
    metadata_path = os.path.join(root, "project.json")
    msg = f"Cannot find {metadata_path}"
    assert_true(os.path.exists(metadata_path), msg)

    if require_local_data:
        print("Validating locally stored data.")
    if require_remote_data:
        print("Validating remotely stored data. (= data stored on s3)")

    with open(metadata_path) as f:
        project_metadata = json.load(f)

    # static validation
    validate_with_schema(project_metadata, "project")

    # dynamic validation
    check_version(SPEC_VERSION, project_metadata["specVersion"], assert_equal)

    datasets = project_metadata["datasets"]
    default_dataset = project_metadata["defaultDataset"]
    msg = f"Cannot find default dataset {default_dataset} in {datasets}"
    assert_in(default_dataset, datasets, msg)

    for dataset in datasets:
        dataset_folder = os.path.join(root, dataset)
        msg = f"Cannot find a dataset {dataset} at {dataset_folder}"
        assert_true(os.path.isdir(dataset_folder), msg)
        validate_dataset(
            dataset_folder,
            require_local_data=require_local_data,
            require_remote_data=require_remote_data,
            assert_true=assert_true,
            assert_in=assert_in,
            assert_equal=assert_equal,
        )
    print("The project at", root, "is a valid MoBIE project.")


def main():
    """@private
    """
    parser = argparse.ArgumentParser("Validate MoBIE project metadata")
    parser.add_argument("--input", "-i", type=str, required=True, help="the project location")
    parser.add_argument("--require_local_data", "-r", type=int, default=1, help="check that local data exists")
    parser.add_argument("--require_remote_data", "-d", type=int, default=0, help="check that remote data exists")
    args = parser.parse_args()
    validate_project(
        args.input, require_local_data=bool(args.require_local_data), require_remote_data=bool(args.require_remote_data)
    )
