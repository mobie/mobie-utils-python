import json
import os
from copy import deepcopy

from .image_dict import load_image_dict
from .datasets import get_datasets
from ..xml_utils import copy_xml_as_n5_s3


def add_remote_project_metadata(
    root,
    bucket_name,
    service_endpoint,
    authentication='Anonymous',
    region='us-west-2'
):
    """ Add metadata to upload remote version of project.

    Arguments:
        root [str] - root data folder of the project
        bucket_name [str] - name of the bucket
        service_endpoint [str] - url of the s3 service end-point.
            For EMBL: 'https://s3.embl.de'.
        authentication [str] - the authentication mode, can be 'Anonymous' or 'Protected'.
            Default: 'Anonymous'
        region [str] - the region. Only relevant if aws.s3 is used.
            Default: 'us-west-2'
    """
    datasets = get_datasets(root)
    for dataset_name in datasets:
        add_remote_dataset_metadata(root, dataset_name, bucket_name,
                                    service_endpoint=service_endpoint,
                                    authentication=authentication,
                                    region=region)

    # TODO print instructions on how to upload the data


def add_remote_dataset_metadata(
    root,
    dataset_name,
    bucket_name,
    service_endpoint,
    authentication='Anonymous',
    region='us-west-2'
):
    """ Add metadata to upload remote version of dataset.

    Arguments:
        root [str] - root data folder of the project
        dataset_name [str] - name of the dataset
        bucket_name [str] - name of the bucket
        service_endpoint [str] - url of the s3 service end-point.
            For EMBL: 'https://s3.embl.de'.
        authentication [str] - the authentication mode, can be 'Anonymous' or 'Protected'.
            Default: 'Anonymous'
        region [str] - the region. Only relevant if aws.s3 is used.
            Default: 'us-west-2'
    """
    if authentication not in ('Anonymous', 'Protected'):
        raise ValueError(f"Expected authentication to be one of Anonymous, Protected, got {authentication}")
    bdv_type = 'bdv.n5.s3'

    dataset_folder = os.path.join(root, dataset_name)
    image_dict_path = os.path.join(dataset_folder, 'images', 'images.json')
    image_dict = load_image_dict(image_dict_path)
    new_image_dict = deepcopy(image_dict)

    for name, settings in image_dict.items():
        # the xml paths for the image dict,
        # which are relative to the 'images' folder
        storage = settings['storage']
        xml = storage['local']
        xml_remote = xml.replace('local', 'remote')

        # the absolute xml paths
        xml_path = os.path.join(dataset_folder, 'images', xml)
        xml_remote_path = os.path.join(dataset_folder, 'images', xml_remote)

        path_in_bucket = os.path.join(dataset_name, 'images', xml.replace('.xml', '.n5'))

        # copy to the xml for remote data
        copy_xml_as_n5_s3(xml_path, xml_remote_path,
                          service_endpoint=service_endpoint,
                          bucket_name=bucket_name,
                          path_in_bucket=path_in_bucket,
                          authentication=authentication,
                          region=region,
                          bdv_type=bdv_type)

        # add the remote storage to the image dict
        storage['remote'] = xml_remote
        settings['storage'] = storage
        new_image_dict[name] = settings

    with open(image_dict_path, 'w') as f:
        json.dump(new_image_dict, f, sort_keys=True, indent=2)
