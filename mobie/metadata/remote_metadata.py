import argparse
import os
import subprocess
from copy import deepcopy
from warnings import warn

from pybdv.metadata import get_data_path

from .dataset_metadata import read_dataset_metadata, write_dataset_metadata
from .project_metadata import get_datasets, read_project_metadata, project_exists, write_project_metadata
from ..xml_utils import copy_xml_as_n5_s3, read_path_in_bucket


def add_remote_project_metadata(
    root,
    bucket_name,
    service_endpoint,
    region=""
):
    """ Add metadata to upload remote version of project.

    Arguments:
        root [str] - root data folder of the project
        bucket_name [str] - name of the bucket
        service_endpoint [str] - url of the s3 service end-point,  e.g. for EMBL: "https://s3.embl.de".
        region [str] - the signing region. Only relevant if aws.s3 is used. (default: "")
    """
    assert project_exists(root), f"Cannot find MoBIE project at {root}"
    datasets = get_datasets(root)
    new_file_formats = None
    for dataset_name in datasets:
        this_new_file_formats = add_remote_dataset_metadata(root, dataset_name, bucket_name,
                                                            service_endpoint=service_endpoint,
                                                            region=region)
        if new_file_formats is None:
            new_file_formats = this_new_file_formats
        else:
            assert len(set(new_file_formats) - set(this_new_file_formats)) == 0

    metadata = read_project_metadata(root)
    file_formats = metadata["imageDataFormats"]
    file_formats = list(set(file_formats).union(set(new_file_formats)))
    metadata["imageDataFormats"] = file_formats
    write_project_metadata(root, metadata)


def _to_bdv_s3(file_format,
               dataset_folder, dataset_name, storage,
               service_endpoint, bucket_name, region):
    new_format = file_format + ".s3"
    os.makedirs(os.path.join(dataset_folder, "images", new_format.replace(".", "-")), exist_ok=True)

    xml = storage["relativePath"]
    xml_remote = xml.replace(file_format.replace(".", "-"), new_format.replace(".", "-"))

    # the absolute xml paths
    xml_path = os.path.join(dataset_folder, xml)
    xml_remote_path = os.path.join(dataset_folder, xml_remote)
    data_rel_path = os.path.join(os.path.split(xml)[0], get_data_path(xml_path))
    data_abs_path = os.path.join(dataset_folder, data_rel_path)
    if not os.path.exists(data_abs_path):
        warn(f"Could not find data path at {data_abs_path} corresponding to xml {xml_path}")
    path_in_bucket = os.path.join(dataset_name, data_rel_path)

    # copy to the xml for remote data
    copy_xml_as_n5_s3(xml_path, xml_remote_path,
                      service_endpoint=service_endpoint,
                      bucket_name=bucket_name,
                      path_in_bucket=path_in_bucket,
                      region=region,
                      bdv_type=new_format)
    return new_format, {"relativePath": xml_remote}


def _to_ome_zarr_s3(dataset_folder, dataset_name, storage,
                    service_endpoint, bucket_name, region):
    rel_path = storage["relativePath"]
    abs_path = os.path.join(dataset_folder, rel_path)
    if not os.path.exists(abs_path):
        warn(f"Could not find dataat {abs_path}")
    # build the s3 address
    s3_address = "/".join([
        service_endpoint.rstrip("/"),
        bucket_name,
        dataset_name,
        rel_path
    ])
    address = {"s3Address": s3_address}
    if region != "":
        address["region"] = region
    return "ome.zarr.s3", address


def add_remote_source_metadata(metadata, new_file_formats,
                               dataset_folder, dataset_name,
                               service_endpoint, bucket_name, region=""):
    new_metadata = deepcopy(metadata)
    source_type = list(metadata.keys())[0]

    for file_format, storage in metadata[source_type]["imageData"].items():
        if file_format == "bdv.n5":
            new_format, s3_storage = _to_bdv_s3(file_format, dataset_folder, dataset_name, storage,
                                                service_endpoint, bucket_name, region)
            new_metadata[source_type]["imageData"][new_format] = s3_storage
            new_file_formats.add(new_format)
        elif file_format == "ome.zarr":
            new_format, s3_storage = _to_ome_zarr_s3(dataset_folder, dataset_name, storage,
                                                     service_endpoint, bucket_name, region)
            new_metadata[source_type]["imageData"][new_format] = s3_storage
            new_file_formats.add(new_format)
        else:
            warn(f"Data in the {file_format} format cannot be uploaded to s3.")

    return new_metadata, new_file_formats


def add_remote_dataset_metadata(
    root,
    dataset_name,
    bucket_name,
    service_endpoint,
    region=""
):
    """ Add metadata to upload remote version of dataset.

    Arguments:
        root [str] - root data folder of the project
        dataset_name [str] - name of the dataset
        bucket_name [str] - name of the bucket
        service_endpoint [str] - url of the s3 service end-point,  e.g. for EMBL: "https://s3.embl.de".
        region [str] - the signing region. Only relevant if aws.s3 is used. (default: "")
    """

    dataset_folder = os.path.join(root, dataset_name)
    ds_metadata = read_dataset_metadata(dataset_folder)
    sources = ds_metadata["sources"]
    new_sources = deepcopy(sources)

    new_file_formats = set()

    for name, metadata in sources.items():
        new_metadata, new_file_formats = add_remote_source_metadata(metadata, new_file_formats,
                                                                    dataset_folder, dataset_name,
                                                                    service_endpoint, bucket_name, region)
        new_sources[name] = new_metadata

    ds_metadata["sources"] = new_sources
    write_dataset_metadata(dataset_folder, ds_metadata)

    return list(new_file_formats)


def upload_source(dataset_folder, metadata, data_format, bucket_name, s3_prefix="embl", client="minio"):
    if data_format.endswith(".s3"):
        base_format = data_format.rstrip(".s3")
        raise ValueError(f"Cannot upload data in format {data_format}, use format {base_format} instead.")
    s3_format = data_format + ".s3"

    if data_format.startswith("bdv"):
        local_xml = os.path.join(dataset_folder, metadata["image"]["imageData"][data_format]["relativePath"])
        remote_xml = os.path.join(dataset_folder, metadata["image"]["imageData"][s3_format]["relativePath"])

        data_path = get_data_path(local_xml, return_absolute_path=True)
        path_in_bucket = read_path_in_bucket(remote_xml)

    elif data_format == "ome.zarr":
        data_path = os.path.join(dataset_folder, metadata["image"]["imageData"][data_format]["relativePath"])
        s3_address = metadata["image"]["imageData"][s3_format]["s3Address"]
        bucket_end_pos = s3_address.find(bucket_name) + len(bucket_name) + 1
        path_in_bucket = s3_address[bucket_end_pos:]

    else:
        raise ValueError(f"Invalid data format {data_format}")

    if client != "minio":
        raise ValueError(f"Invalid client {client}, currently only minio is supported")

    assert os.path.exists(data_path)
    cmd = ["mc", "cp", "-r", f"{data_path}/", f"{s3_prefix}/{bucket_name}/{path_in_bucket}/"]
    subprocess.run(cmd)


def main():
    parser = argparse.ArgumentParser("Add remote metadata to a MoBIE project so that it can be accessed via S3.")
    parser.add_argument("-i", "--input", help="MoBIE project folder", required=True)
    parser.add_argument("-b", "--bucket_name", required=True,
                        help="Name of the bucket where the dataset will be uploaded")
    parser.add_argument("-s", "--service_endpoint", required=True,
                        help="The url of the s3 service endpoint, e.g. 'https://s3.embl.de' for the EMBL s3")
    parser.add_argument("--region", help="The aws signing region (only relevant if uploading to aws s3)",
                        default="")
    args = parser.parse_args()
    add_remote_project_metadata(args.input, args.bucket_name, args.service_endpoint, args.region)
