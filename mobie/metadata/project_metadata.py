import os
import warnings
from .utils import read_metadata, write_metadata
from ..__version__ import SPEC_VERSION

#
# functionality for reading / writing project.schema.json
#


def create_project_metadata(root, file_formats, description=None, references=None):
    os.makedirs(root, exist_ok=True)
    path = os.path.join(root, "project.json")
    if os.path.exists(path):
        raise RuntimeError(f"Project metadata at {path} already exists")
    metadata = {
        "specVersion": SPEC_VERSION,
        "imageDataFormats": file_formats,
        "datasets": []
    }
    if description is not None:
        metadata[description] = description
    if references is not None:
        metadata[references] = references
    write_project_metadata(root,  metadata)


def read_project_metadata(root):
    path = os.path.join(root, "project.json")
    return read_metadata(path)


def write_project_metadata(root, metadata):
    path = os.path.join(root, "project.json")
    write_metadata(path, metadata)


#
# query project for datasets etc.
#


def project_exists(root):
    meta = read_project_metadata(root)
    required_fields = ["datasets", "imageDataFormats", "specVersion"]
    return all(req in meta for req in required_fields)


def dataset_exists(root, dataset_name):
    project = read_project_metadata(root)
    return dataset_name in project.get("datasets", [])


def add_dataset(root, dataset_name, is_default):
    project = read_project_metadata(root)

    if dataset_name in project["datasets"]:
        warnings.warn(f"Dataset {dataset_name} is already present!")
    else:
        project["datasets"].append(dataset_name)

    # if this is the only dataset we set it as default
    if is_default or len(project["datasets"]) == 1:
        project["defaultDataset"] = dataset_name

    write_project_metadata(root, project)


def get_datasets(root):
    return read_project_metadata(root)["datasets"]


def get_file_formats(root):
    metadata = read_project_metadata(root)
    return metadata["imageDataFormats"]


def has_file_format(root, file_format):
    file_formats = get_file_formats(root)
    return file_format in file_formats
