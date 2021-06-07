import os

from .dataset_metadata import read_dataset_metadata
from .project_metadata import get_datasets, read_project_metadata, write_project_metadata
from ..xml_utils import add_s3_to_xml


def _add_s3_to_project(root, bucket_name, service_endpoint, region):
    metadata = read_project_metadata(root)
    s3_roots = metadata.get('s3Root', [])
    s3_roots.append({
        "endpoint": service_endpoint,
        "bucket": bucket_name,
        "region": region
    })
    metadata['s3Root'] = s3_roots
    write_project_metadata(root, metadata)


def add_remote_project_metadata(
    root,
    bucket_name,
    service_endpoint,
    region='us-west-2'
):
    """ Add metadata to upload remote version of project.

    Arguments:
        root [str] - root data folder of the project
        bucket_name [str] - name of the bucket
        service_endpoint [str] - url of the s3 service end-point,  e.g. for EMBL: 'https://s3.embl.de'.
        region [str] - the region. Only relevant if aws.s3 is used. (default: 'us-west-2')
    """
    datasets = get_datasets(root)
    for dataset_name in datasets:
        add_remote_dataset_metadata(root, dataset_name, bucket_name,
                                    service_endpoint=service_endpoint,
                                    region=region)
    _add_s3_to_project(root, bucket_name, service_endpoint, region)

    # TODO print instructions on how to upload the data


def add_remote_dataset_metadata(
    root,
    dataset_name,
    bucket_name,
    service_endpoint,
    region='us-west-2'
):
    """ Add metadata to upload remote version of dataset.

    Arguments:
        root [str] - root data folder of the project
        dataset_name [str] - name of the dataset
        bucket_name [str] - name of the bucket
        service_endpoint [str] - url of the s3 service end-point,  e.g. for EMBL: 'https://s3.embl.de'.
        region [str] - the region. Only relevant if aws.s3 is used. (default: 'us-west-2')
    """

    dataset_folder = os.path.join(root, dataset_name)
    ds_metadata = read_dataset_metadata(dataset_folder)
    sources = ds_metadata["sources"]

    for name, metadata in sources.items():
        source_type = list(metadata.keys())[0]

        # the xml path for the source, which are relative to the dataset root folder
        xml = metadata[source_type]['imageData']['relativePath']
        # the absolute xml path
        xml_path = os.path.join(dataset_folder, xml)
        path_in_bucket = os.path.join(dataset_name, xml.replace('.xml', '.n5'))

        # copy to the xml for remote data
        add_s3_to_xml(xml_path,
                      service_endpoint=service_endpoint,
                      bucket_name=bucket_name,
                      path_in_bucket=path_in_bucket,
                      region=region)
