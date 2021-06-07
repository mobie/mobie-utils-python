import os
from copy import deepcopy

from .dataset_metadata import read_dataset_metadata, write_dataset_metadata
from .project_metadata import get_datasets, read_project_metadata, write_project_metadata
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
    file_formats = metadata['imageDataFormats']
    file_formats = list(set(file_formats).union(set(new_file_formats)))
    metadata['imageDataFormats'] = file_formats
    write_project_metadata(root, metadata)


def _to_bdv_n5_s3(dataset_folder, dataset_name, storage,
                  service_endpoint, bucket_name, region):
    os.makedirs(os.path.join(dataset_folder, 'images', 'bdv.n5.s3'), exist_ok=True)

    xml = storage['relativePath']
    xml_remote = xml.replace('bdv.n5', 'bdv.n5.s3')

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
                      bdv_type='bdv.n5.s3')
    return {'relativePath': xml_remote}


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
    new_sources = deepcopy(sources)

    new_file_formats = set()

    for name, metadata in sources.items():
        new_metadata = deepcopy(metadata)
        source_type = list(metadata.keys())[0]

        for file_format, storage in metadata[source_type]['imageData'].items():
            # currently we only know how to add s3 data for bdv.n5
            if file_format == 'bdv.n5':
                s3_storage = _to_bdv_n5_s3(dataset_folder, dataset_name, storage,
                                           service_endpoint, bucket_name, region)
                new_metadata[source_type]['imageData']['bdv.n5.s3'] = s3_storage
                new_file_formats.add('bdv.n5.s3')

        new_sources[name] = new_metadata

    ds_metadata["sources"] = new_sources
    write_dataset_metadata(dataset_folder, ds_metadata)

    return list(new_file_formats)
