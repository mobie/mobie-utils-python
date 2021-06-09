import argparse
import json
import os

import mobie.metadata as metadata
import mobie.s3_utils as s3_utils
from mobie.validation import validate_project


def parse_address(address):
    if not address.endswith('.n5'):
        raise ValueError(f"Could not parse open organelle address {address}")
    parts = address.split("/")
    if len(parts) != 5 or parts[1] != '':
        raise ValueError(f"Could not parse open organelle address {address}")
    endpoint = parts[0] + "//" + parts[2]
    bucket = parts[3]
    name = parts[4]
    return endpoint, bucket, name


def get_source(client, bucket, container, source_name,
               endpoint, dataset_folder):
    source_object = os.path.join(container, source_name, 'attributes.json')
    attrs_file = s3_utils.download_file(client, bucket, source_object)
    with open(attrs_file) as f:
        attrs = json.load(f)
    name = attrs['name']

    # for now we hard-code the source type to image.
    # in the future it would be nice to infer this somehow from the attributes
    source_type = 'image'

    # get the mobie source metadata
    address = os.path.join(endpoint, bucket, container, source_name)
    if source_type == 'image':
        source = metadata.get_image_metadata(dataset_folder, address,
                                             file_format='openOrganelle.s3')
    else:
        source = metadata.get_segmentation_metadata(dataset_folder, address,
                                                    file_format='openOrganelle.s3')

    # get the mobie view metadata
    # we infer the menu_name from the root of the source name
    menu_name = os.path.split(source_name)[0]
    if menu_name == '':
        menu_name = name
    # TODO infer the contrast limits
    view = metadata.get_default_view(source_type, name, menu_name=menu_name)

    return name, source, view


# TODO make source names optional and discover them if not given
# - it would be nice to have some list of all available sources per container instead of doing this via s3
# -> ask John about this
def add_open_organelle_dataset(address, root,
                               source_names,
                               dataset_name=None,
                               # region="us-west-2",  # we don't seem to need this
                               anon=True,
                               is_default=False):
    """
    """
    if not s3_utils.have_boto():
        raise RuntimeError("boto3 is required to access open organelle data. Please install it.")

    file_format = 'openOrganelle.s3'
    if not metadata.project_exists(root):
        metadata.create_project_metadata(root, [file_format])

    endpoint, bucket, container = parse_address(address)
    dataset_name = bucket if dataset_name is None else dataset_name

    ds_exists = metadata.dataset_exists(root, dataset_name)
    ds_folder = os.path.join(root, dataset_name)
    if ds_exists:
        ds_metadata = metadata.read_dataset_metadata(ds_folder)
        sources, views = ds_metadata['sources'], ds_metadata['views']
    else:
        sources, views = {}, {}

    client = s3_utils.get_client(endpoint, anon=anon)
    for source_name in source_names:
        name, source, view = get_source(client, bucket, container, source_name,
                                        endpoint, ds_folder)
        if name in sources:
            continue
        sources[name] = source
        views[name] = view

    if ds_exists:
        ds_metadata['sources'] = sources
        ds_metadata['views'] = views
        metadata.write_dataset_metadata(ds_folder, ds_metadata)
    else:
        os.makedirs(ds_folder, exist_ok=True)
        views['default'] = views[list(views.keys())[0]]
        metadata.create_dataset_metadata(ds_folder, sources=sources, views=views)
        metadata.add_dataset(root, dataset_name, is_default)

    validate_project(root)


def main():
    description = ""
    parser = argparse.ArgumentParser(description)
    parser.add_argument('--address', type=str, required=True)
    parser.add_argument('--root', type=str, required=True)
    parser.add_argument('--source_names', type=str, nargs="+", required=True)
    parser.add_argument('--dataset_name', type=str, default=None)
    args = parser.parse_args()
    add_open_organelle_dataset(args.address, args.root)
