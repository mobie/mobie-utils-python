import json
import os
from .migrate_dataset import migrate_dataset
from ...metadata import write_project_metadata
from ...validation import validate_project


def migrate_project(root,):

    project_file = os.path.join(root, "project.json")
    with open(project_file, "r") as f:
        metadata = json.load(f)
    assert metadata["specVersion"] == "0.2.0",\
        f"Expected spec version 0.2.0 for migration, got {metadata['specVersion']}"

    for ds in metadata["datasets"]:
        ds_folder = os.path.join(root, ds)
        assert os.path.exists(ds_folder), ds_folder
        print("Migrate dataset:", ds)
        migrate_dataset(ds_folder)

    # write spec version and remove imageDataFormats, which is not needed any more
    metadata["specVersion"] = "0.3.0"
    metadata.pop("imageDataFormats")

    write_project_metadata(root, metadata)
    validate_project(root, require_local_data=False, require_remote_data=False)
