import os
import warnings
from .dataset_metadata import read_dataset_metadata, write_dataset_metadata
from .view_metadata import get_default_view
from ..validation import validate_source_metadata


def get_image_metadata(source_name, xml_path, view=None):
    if view is None:
        view = get_default_view("image", source_name)
    source_metadata = {
        "image": {
            "imageDataLocations": {
                "local": xml_path
            },
            "view": view
        }
    }
    return source_metadata


def get_segmentation_metadata(source_name, xml_path, view=None, table_location=None):
    if view is None:
        view = get_default_view("image", source_name)
    source_metadata = {
        "segmentation": {
            "imageDataLocations": {
                "local": xml_path
            },
            "view": view
        }
    }
    if table_location is not None:
        source_metadata["segmentation"]["tableDataLocation"] = table_location
    return source_metadata


def add_source_metadata(
    dataset_folder,
    source_type,
    source_name,
    xml_path,
    view=None,
    table_folder=None,
    overwrite=True
):
    """ Add metadata entry for a souce to MoBIE dataset.

    Arguments:
        dataset_folder [str] - path to the dataset folder.
        source_type [str] - type of the source, either 'image' or 'segmentation'.
        source_name [str] - name of the source.
        xml_path [str] - path to the xml for the image data corresponding to this source.
        view [dict] - default view for this source. (default: None)
        table_folder [str] - table folder for segmentations. (default: None)
        overwrite [bool] - whether to overwrite existing entries (default: True)
    """
    dataset_metadata = read_dataset_metadata(dataset_folder)
    sources_metadata = dataset_metadata["sources"]

    # validate the arguments
    if source_type not in ('image', 'segmentation'):
        raise ValueError(f"Expect source_type to be 'image' or 'segmentation', got {source_type}")

    if source_name in sources_metadata:
        msg = f"A source with name {source_name} already exists for the dataset {dataset_folder}"
        if overwrite:
            warnings.warn(msg)
        else:
            raise ValueError(msg)

    # create the metadata for this source
    relative_xml_path = os.path.relpath(xml_path, dataset_folder)

    if source_type == "image":
        source_metadata = get_image_metadata(source_name, relative_xml_path, view)
    else:
        relative_table_folder = None if table_folder is None else os.path.relpath(table_folder, dataset_folder)
        source_metadata = get_segmentation_metadata(source_name, relative_xml_path,
                                                    view, relative_table_folder)

    validate_source_metadata(source_name, source_metadata, dataset_folder)
    sources_metadata[source_name] = source_metadata
    dataset_metadata["sources"] = sources_metadata
    write_dataset_metadata(dataset_folder, dataset_metadata)


# TODO
def update_source_metadata():
    pass
