import argparse
import json
import os
from .dataset import validate_dataset
from .utils import _assert_true, _assert_in, _assert_equal
from ..__version__ import SPEC_VERSION


def check_version(version_a, version_b, assert_equal):

    def parse_version(version):
        version_split = version.split('.')
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


def validate_project(root, assert_true=_assert_true, assert_in=_assert_in, assert_equal=_assert_equal):
    datasets_file = os.path.join(root, "datasets.json")
    msg = f"Cannot find {datasets_file}"
    assert_true(os.path.exists(datasets_file), msg)

    with open(datasets_file) as f:
        dataset_metadata = json.load(f)

    msg = f"Cannot find 'version' field in dataset metadata at {datasets_file}"
    assert_in("specVersion", dataset_metadata, msg)
    check_version(SPEC_VERSION, dataset_metadata["specVersion"], assert_equal)

    datasets = dataset_metadata["datasets"]
    default_dataset = dataset_metadata["defaultDataset"]
    msg = f"Cannot find default dataset {default_dataset} in {datasets}"
    assert_in(default_dataset, datasets, msg)

    for dataset in datasets:
        dataset_folder = os.path.join(root, dataset)
        msg = f"Cannot find a dataset {dataset} at {dataset_folder}"
        assert_true(os.path.isdir(dataset_folder), msg)
        validate_dataset(dataset_folder, assert_true=assert_true, assert_in=assert_in)


def main():
    parser = argparse.ArgumentParser("Validate MoBIE project metadata")
    parser.add_argument('--input', '-i', type=str, required=True,
                        help="the project location")
    args = parser.parse_args()
    validate_project(args.input)
