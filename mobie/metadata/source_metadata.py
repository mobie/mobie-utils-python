import os
import warnings
from pybdv.metadata import get_bdv_format
from .dataset_metadata import read_dataset_metadata, write_dataset_metadata
from .utils import get_table_metadata
from .view_metadata import get_default_view
from ..validation import validate_source_metadata, validate_view_metadata


def _get_file_format(path):
    if not os.path.exists(path):
        raise ValueError(f"{path} does not exist.")
    elif path.endswith('.xml'):
        file_format = get_bdv_format(path)
    elif path.endswith('.ome.zarr'):
        file_format = 'ome.zarr'
    else:
        raise ValueError(f"Could not infer file format from {path}.")
    return file_format


def _get_image_metadata(dataset_folder, path, type_, file_format, description):
    file_format = _get_file_format(path) if file_format is None else file_format

    if file_format.startswith("bdv"):
        format_ = {"relativePath": os.path.relpath(path, dataset_folder)}
    elif file_format == "ome.zarr":
        format_ = {"relativePath": os.path.relpath(path, dataset_folder)}
    # TODO support (optional) signing address for s3 formats?
    elif file_format == "ome.zarr.s3":
        format_ = {"s3Address": path}
    elif file_format == "openOrganelle.s3":
        format_ = {"s3Address": path}
    else:
        raise ValueError(f"Invalid file format {file_format}")

    source_metadata = {"imageData": {file_format: format_}}
    if description is not None:
        assert isinstance(description, str)
        source_metadata["description"] = description
    return {type_: source_metadata}


def get_image_metadata(dataset_folder, metadata_path,
                       file_format=None, description=None):
    return _get_image_metadata(dataset_folder, metadata_path, "image",
                               file_format=file_format, description=description)


def get_segmentation_metadata(dataset_folder, metadata_path,
                              table_location=None, file_format=None, description=None):
    source_metadata = _get_image_metadata(dataset_folder, metadata_path, "segmentation",
                                          file_format=file_format, description=description)
    if table_location is not None:
        relative_table_location = os.path.relpath(table_location, dataset_folder)
        source_metadata["segmentation"]["tableData"] = get_table_metadata(relative_table_location)
    return source_metadata


def add_source_to_dataset(
    dataset_folder,
    source_type,
    source_name,
    image_metadata_path,
    view=None,
    table_folder=None,
    overwrite=True,
    description=None
):
    """ Add source metadata to a MoBIE dataset.

    Arguments:
        dataset_folder [str] - path to the dataset folder.
        source_type [str] - type of the source, either 'image' or 'segmentation'.
        source_name [str] - name of the source.
        xml_path [str] - path to the xml for the image data corresponding to this source.
        view [dict] - view for this source. If None, will create a default view.
            If empty dict, will not add a view (default: None)
        table_folder [str] - table folder for segmentations. (default: None)
        overwrite [bool] - whether to overwrite existing entries (default: True)
        description [str] - description for this source (default: None)
    """
    dataset_metadata = read_dataset_metadata(dataset_folder)
    sources_metadata = dataset_metadata["sources"]
    view_metadata = dataset_metadata["views"]

    # validate the arguments
    if source_type not in ('image', 'segmentation'):
        raise ValueError(f"Expect source_type to be 'image' or 'segmentation', got {source_type}")

    if source_name in sources_metadata or source_name in view_metadata:
        msg = f"A source with name {source_name} already exists for the dataset {dataset_folder}"
        if overwrite:
            warnings.warn(msg)
        else:
            raise ValueError(msg)

    if source_type == "image":
        source_metadata = get_image_metadata(dataset_folder, image_metadata_path,
                                             description=description)
    else:
        source_metadata = get_segmentation_metadata(dataset_folder,
                                                    image_metadata_path,
                                                    table_folder,
                                                    description=description)
    validate_source_metadata(source_name, source_metadata, dataset_folder)
    sources_metadata[source_name] = source_metadata
    dataset_metadata["sources"] = sources_metadata

    if view != {}:
        if view is None:
            view = get_default_view(source_type, source_name)
        validate_view_metadata(view)
        view_metadata[source_name] = view
        dataset_metadata["views"] = view_metadata

    write_dataset_metadata(dataset_folder, dataset_metadata)


# TODO
def update_source_metadata():
    pass
