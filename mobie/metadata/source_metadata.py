import json
import os
import warnings

import elf.transformation as trafo_utils
import s3fs
from pybdv import metadata as bdv_metadata

from .dataset_metadata import read_dataset_metadata, write_dataset_metadata
from .utils import get_table_metadata
from .view_metadata import get_default_view
from ..validation import validate_source_metadata, validate_view_metadata


#
# functionality for querying source metadata
#


def _load_bdv_metadata(dataset_folder, storage):
    xml_path = os.path.join(dataset_folder, storage["relativePath"])
    return xml_path


def _load_json_from_s3(address):
    server = "/".join(address.split("/")[:3])
    path = "/".join(address.split("/")[3:])
    try:
        fs = s3fs.S3FileSystem(anon=True, client_kwargs={"endpoint_url": server})
        store = s3fs.S3Map(root=path, s3=fs)
        attrs = store[".zattrs"]
    except Exception:
        return None
    attrs = json.loads(attrs.decode("utf-8"))
    return attrs


def _load_json_from_file(path):
    if not os.path.exists(path):
        return None
    with open(path) as f:
        attrs = json.load(f)
    return attrs


def _load_ome_zarr_metadata(dataset_folder, storage, data_format):
    if data_format == "ome.zarr":
        attrs_path = os.path.join(dataset_folder, storage["relativePath"], ".zattrs")
        attrs = _load_json_from_file(attrs_path)
    else:
        assert data_format == "ome.zarr.s3"
        address = storage["s3Address"]
        attrs = _load_json_from_s3(address)
    return None if attrs is None else attrs[0]["multiscales"]


def _load_image_metadata(source_metadata, dataset_folder):
    image_metadata = None
    for data_format, storage in source_metadata.items():
        if data_format.startswith("bdv"):
            image_metadata = _load_bdv_metadata(dataset_folder, storage)
        elif data_format.startswith("ome.zarr"):
            image_metadata = _load_ome_zarr_metadata(dataset_folder, storage, data_format)
        if image_metadata is not None:
            return data_format, image_metadata
    raise RuntimeError(f"Could not load the image metadata for {image_metadata}")


def get_shape(source_metadata, dataset_folder):
    data_format, image_metadata = _load_image_metadata(source_metadata, dataset_folder)
    if data_format.startswith("bdv"):
        shape = bdv_metadata.get_size(image_metadata, setup_id=0)
    elif data_format == "ome.zarr":
        dataset_path = image_metadata["datasets"][0]["path"]
        array_path = os.path.join(
            dataset_folder, source_metadata["storage"][data_format]["relativePath"], dataset_path, ".zarray"
        )
        array_metadata = _load_json_from_file(array_path)
        shape = array_metadata["shape"]
    elif data_format == "ome.zarr.s3":
        dataset_path = image_metadata["datasets"][0]["path"]
        address = source_metadata[data_format]["s3Address"]
        array_address = os.path.join(address, dataset_path, ".zarray")
        array_metadata = _load_json_from_s3(array_address)
        shape = array_metadata["shape"]
    else:
        raise ValueError(f"Unsupported data format {data_format}")
    return shape


def _bdv_transform_to_affine_matrix(transforms, resolution):
    assert isinstance(transforms, dict)
    transforms = list(transforms.values())
    # TODO do we need to pass the resolution here ????
    transforms = [trafo_utils.bdv_to_native(trafo, resolution=resolution) for trafo in transforms]
    # TODO is this the correct order of concatenation?
    transform = transforms[0]
    for trafo in transforms[1:]:
        transform = transform @ trafo
    return transform


# load the transformation from the metadata of this source
def get_transformation(source_metadata, dataset_folder, to_affine_matrix=True, resolution=None):
    data_format, image_metadata = _load_image_metadata(source_metadata, dataset_folder)
    if data_format.startswith("bdv"):
        transform = bdv_metadata.get_affine(image_metadata, setup_id=0)
        if to_affine_matrix:
            # TODO
            if resolution is None:
                pass
            transform = _bdv_transform_to_affine_matrix(transform, resolution)
    elif data_format.startswith("ome.zarr"):
        if to_affine_matrix:
            transform = trafo_utils.ngff_to_native(image_metadata)
    else:
        raise ValueError(f"Unsupported data format {data_format}")
    return transform


def get_resolution(source_metadata, dataset_folder):
    data_format, image_metadata = _load_image_metadata(source_metadata, dataset_folder)
    if data_format.startswith("bdv"):
        resolution = bdv_metadata.get_resolution(image_metadata, setup_id=0)
    elif data_format.startswith("ome.zarr"):
        transforms = image_metadata["datasets"][0]["coordinateTransformations"]
        resolution = [1.0, 1.0, 1.0]
        for trafo in transforms:
            if trafo["type"] == "scale":
                resolution = trafo["scale"]
    else:
        raise ValueError(f"Unsupported data format {data_format}")
    return resolution


#
# functionality for creating source metadata and adding it to datasets
#


def _get_file_format(path):
    if not os.path.exists(path):
        raise ValueError(f"{path} does not exist.")
    elif path.endswith(".xml"):
        file_format = bdv_metadata.get_bdv_format(path)
    elif path.endswith(".ome.zarr"):
        file_format = "ome.zarr"
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
        image_metadata_path [str] - path to the image metadata (like BDV-XML) corresponding to this source.
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
    if source_type not in ("image", "segmentation"):
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
