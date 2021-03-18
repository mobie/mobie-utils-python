import os
import warnings
from .utils import read_metadata, write_metadata
from .view_metadata import get_default_view


def write_sources_metadata(dataset_folder, sources_metadata):
    path = os.path.join(dataset_folder, 'sources.json')
    write_metadata(path, sources_metadata)


def read_sources_metadata(dataset_folder):
    path = os.path.join(dataset_folder, 'sources.json')
    return read_metadata(path)


def add_source_metadata(
    dataset_folder,
    source_type,
    xml_path,
    menu_item,
    view=None,
    table_folder=None,
    overwrite=True
):
    """ Add metadata entry for a souce to MoBIE dataset.

    Arguments:
        dataset_folder [str] - path to the dataset folder.
        source_type [str] - type of the ource, either 'image' or 'segmentation'.
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
    if not os.path.exists(xml_path):
        raise ValueError(f"Invalid xml_path: {xml_path}")
    if not isinstance(menu_item, str) and len(menu_item.split("/") != 2):
        raise ValueError(f"Expect menu_item to have the format '<MENU>/<ENTRY>', got {menu_item}")
    if table_folder is not None:
        if source_type != "segmentation":
            msg = "Invalid parameter combination: a table folder may only be specified for sources of type segmentation"
            raise ValueError(msg)
        if not os.path.isdir(table_folder):
            raise ValueError(f"Invalid table folder: {table_folder}")

    source_name = os.path.splitext(os.path.split(xml_path)[1])[0]
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
    # TODO validate the view metadata with json schema

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

    # TODO validate source_metadata with json schema

    sources_metadata[source_name] = source_metadata
    write_sources_metadata(dataset_folder, sources_metadata)


# TODO
def update_source_metadata():
    pass
