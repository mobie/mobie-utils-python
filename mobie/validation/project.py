import argparse
import json
import os
from .dataset import validate_dataset
from .utils import _assert_true, _assert_in, _assert_equal, validate_with_schema
from ..__version__ import SPEC_VERSION


def check_version(version_a, version_b, assert_equal):

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


def validate_project(root,
                     require_data=True,
                     assert_true=_assert_true,
                     assert_in=_assert_in,
                     assert_equal=_assert_equal):
    metadata_path = os.path.join(root, "project.json")
    msg = f"Cannot find {metadata_path}"
    assert_true(os.path.exists(metadata_path), msg)

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
            dataset_folder, require_data=require_data,
            assert_true=assert_true, assert_in=assert_in, assert_equal=assert_equal
        )
    print("The project at", root, "is a valid MoBIE project.")


def main():
    parser = argparse.ArgumentParser("Validate MoBIE project metadata")
    parser.add_argument("--input", "-i", type=str, required=True, help="the project location")
    parser.add_argument("--require_data", "-r", type=int, default=1, help="whether to require that local dat exists")
    args = parser.parse_args()
    validate_project(args.input, require_data=bool(args.require_data))
