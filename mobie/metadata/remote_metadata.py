import os
from copy import deepcopy

from .dataset_metadata import read_dataset_metadata, write_dataset_metadata
from .project_metadata import get_datasets
from ..xml_utils import copy_xml_as_n5_s3


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
    bdv_type = 'bdv.n5.s3'

    dataset_folder = os.path.join(root, dataset_name)
    ds_metadata = read_dataset_metadata(dataset_folder)
    sources = ds_metadata["sources"]
    new_sources = deepcopy(sources)

    for name, metadata in sources.items():
        source_type = list(metadata.keys())[0]

        # the xml paths for the source,
        # which are relative to the 'images' folder
        storage = metadata[source_type]['imageDataLocations']
        xml = storage['fileSystem']
        xml_remote = xml.replace('local', 'remote')

        # the absolute xml paths
        xml_path = os.path.join(dataset_folder, xml)
        xml_remote_path = os.path.join(dataset_folder, xml_remote)
        path_in_bucket = os.path.join(dataset_name, xml.replace('.xml', '.n5'))

        # copy to the xml for remote data
        copy_xml_as_n5_s3(xml_path, xml_remote_path,
                          service_endpoint=service_endpoint,
                          bucket_name=bucket_name,
                          path_in_bucket=path_in_bucket,
                          region=region,
                          bdv_type=bdv_type)

        # add the remote storage to the source
        storage['s3store'] = xml_remote
        metadata[source_type]['imageDataLocations'] = storage
        new_sources[name] = metadata

    ds_metadata["sources"] = new_sources
    write_dataset_metadata(dataset_folder, ds_metadata)
