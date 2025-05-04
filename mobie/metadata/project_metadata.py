"""Functionality for MoBIE project metadata.
"""
import os
import warnings
from typing import Dict, List, Optional, Sequence

from .utils import read_metadata, write_metadata
from ..__version__ import SPEC_VERSION

#
# functionality for reading / writing project.schema.json
#


def create_project_metadata(
    root: str, description: Optional[str] = None, references: Optional[Sequence[str]] = None
) -> None:
    """

    Args:
        root: The MoBIE project root directory.
        description: The description of this project.
        references: Optional list of publications associated with this project.
    """
    os.makedirs(root, exist_ok=True)
    path = os.path.join(root, "project.json")
    if os.path.exists(path):
        raise RuntimeError(f"Project metadata at {path} already exists")
    metadata = {
        "specVersion": SPEC_VERSION,
        "datasets": []
    }
    if description is not None:
        metadata["description"] = description
    if references is not None:
        metadata["references"] = references
    write_project_metadata(root,  metadata)


def read_project_metadata(root: str) -> Dict:
    """Read project metadata.

    Args:
        root: The project root directory.

    Returns:
        The project metadata.
    """
    path = os.path.join(root, "project.json")
    return read_metadata(path)


def write_project_metadata(root: str, metadata: Dict) -> None:
    """Write project metadata.

    Args:
        root: The project root directory.
        metadata: The project metadata to write.
    """
    path = os.path.join(root, "project.json")
    write_metadata(path, metadata)


#
# query project for datasets etc.
#


def project_exists(root: str) -> bool:
    """Check whether a project exists at the given root directory.

    Args:
        root: The project root directory.

    Returns:
        Whether the project exists.
    """
    meta = read_project_metadata(root)
    required_fields = ["datasets", "specVersion"]
    return all(req in meta for req in required_fields)


def dataset_exists(root: str, dataset_name: str) -> bool:
    """Check whether a dataset exists in the given project.

    Args:
        root: The project root directory.
        dataset_name: The name of the dataset to check for.

    Returns:
        Whether the dataset exists.
    """
    project = read_project_metadata(root)
    return dataset_name in project.get("datasets", [])


def add_dataset(root: str, dataset_name: str, is_default: bool):
    """Add a dataset to a given MoBIE project.

    This only adds the dataset to the project metadata, it does not
    actually create the underlying dataset folder structure.

    Args:
        root: The project root directory.
        dataset_name: The name of the dataset to add.
        is_default: Whether this is the default dataset.
    """
    project = read_project_metadata(root)

    if dataset_name in project["datasets"]:
        warnings.warn(f"Dataset {dataset_name} is already present!")
    else:
        project["datasets"].append(dataset_name)

    # if this is the only dataset we set it as default
    if is_default or len(project["datasets"]) == 1:
        project["defaultDataset"] = dataset_name

    write_project_metadata(root, project)


def get_datasets(root: str) -> List[str]:
    """Get the list of datasets present in a project.

    Args:
        root: The project root directory.

    Returns:
        The list of dataset names.
    """
    return read_project_metadata(root)["datasets"]
