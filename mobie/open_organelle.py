import argparse
import json
import os

import mobie.metadata as metadata
import mobie.s3_utils as s3_utils
from mobie.validation import validate_project, validate_view_metadata


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


def get_source(client, bucket, container, internal_path,
               endpoint, dataset_folder,
               source_name, view, menu_name):
    source_object = os.path.join(container, internal_path, 'attributes.json')
    attrs_file = s3_utils.download_file(client, bucket, source_object)
    with open(attrs_file) as f:
        attrs = json.load(f)
    name = attrs['name'] if source_name is None else source_name

    # for now we hard-code the source type to image.
    # in the future it would be nice to infer this somehow from the attributes
    source_type = 'image'

    # get the mobie source metadata
    address = os.path.join(endpoint, bucket, container, internal_path)
    if source_type == 'image':
        source = metadata.get_image_metadata(dataset_folder, address,
                                             file_format='openOrganelle.s3')
    else:
        source = metadata.get_segmentation_metadata(dataset_folder, address,
                                                    file_format='openOrganelle.s3')

    # get the mobie view metadata

    # if the menu-name was not specified, we infer it from the root of the source name
    if menu_name is None:
        menu_name = os.path.split(internal_path)[0]
        menu_name = name if menu_name == "" else menu_name

    if view is None:
        view = metadata.get_default_view(source_type, name, menu_name=menu_name)
    else:
        view.update({"uiSelectionGroup": menu_name})
    validate_view_metadata(view, sources=[name])

    return name, source, view


# TODO make source names optional and discover them if not given
# - it would be nice to have some list of all available sources per container instead of doing this via s3
# -> ask John about this
def add_open_organelle_data(address, root,
                            internal_path,
                            source_name=None,
                            dataset_name=None,
                            # region="us-west-2",  # we don't seem to need this
                            anon=True,
                            view=None,
                            menu_name=None,
                            is_default_dataset=False,
                            overwrite=False):
    """
        address [str] -
        root [str] -
        internal_path [str] -
        source_name [str] -
        dataset_name [str] -
        anon [bool] -
        view [dict] - default view settings for this source (default: None)
        menu_name [str] - menu name for this source.
            If none will be derived from the source name. (default: None)
        is_default_dataset [bool] -
        overwrite [bool]
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

    name, source, view = get_source(client, bucket, container, internal_path,
                                    endpoint, ds_folder, source_name, view, menu_name)
    if name in sources:
        if overwrite:
            print("The source", name, "exists already and will be over-written")
        else:
            print("The source", name, "exists already and will not be over-written")
            return

    sources[name] = source
    views[name] = view

    if ds_exists:
        ds_metadata['sources'] = sources
        ds_metadata['views'] = views
        metadata.write_dataset_metadata(ds_folder, ds_metadata)
    else:
        os.makedirs(ds_folder, exist_ok=True)
        default_view = views[list(views.keys())[0]]
        default_view["sourceDisplays"]["uiSelectionGroup"] = "bookmarks"
        views["default"] = default_view
        metadata.create_dataset_metadata(ds_folder, sources=sources, views=views)
        metadata.add_dataset(root, dataset_name, is_default_dataset)

    validate_project(root)


def main():
    description = ""
    parser = argparse.ArgumentParser(description)
    parser.add_argument('--address', type=str, required=True)
    parser.add_argument('--root', type=str, required=True)
    parser.add_argument('--internal_path', type=str, required=True)
    parser.add_argument('--source_name', type=str, default=None)
    parser.add_argument('--dataset_name', type=str, default=None)
    args = parser.parse_args()
    add_open_organelle_data(args.address, args.root, args.internal_path,
                            args.source_name, args.dataset_name)
