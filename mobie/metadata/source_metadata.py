import os
import warnings
from .utils import read_metadata, write_metadata
from .view_metadata import get_default_view
from ..validation import validate_source_metadata


def write_sources_metadata(dataset_folder, sources_metadata):
    path = os.path.join(dataset_folder, 'sources.json')
    write_metadata(path, sources_metadata)


def read_sources_metadata(dataset_folder):
    path = os.path.join(dataset_folder, 'sources.json')
    return read_metadata(path)


def add_source_metadata(
    dataset_folder,
    source_type,
    source_name,
    xml_path,
    menu_item,
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
        menu_item [str] -
        view [dict] - default view for this source. (default: None)
        table_folder [str] - table folder for segmentations. (default: None)
        overwrite [bool] - whether to overwrite existing entries (default: True)
    """
    sources_metadata = read_sources_metadata(dataset_folder)

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
    if view is None:
        view = get_default_view(source_type, source_name)

    source_metadata = {
        "imageLocation": {
            "local": relative_xml_path
        },
        "menuItem": menu_item,
        "type": source_type,
        "view": view
    }
    if table_folder is not None:
        relative_table_folder = os.path.relpath(table_folder, dataset_folder)
        source_metadata["tableRootLocation"] = relative_table_folder

    validate_source_metadata(source_name, source_metadata, dataset_folder)
    sources_metadata[source_name] = source_metadata
    write_sources_metadata(dataset_folder, sources_metadata)


# TODO
def update_source_metadata():
    pass
