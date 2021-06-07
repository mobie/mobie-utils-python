import os
from copy import deepcopy
from subprocess import run

import mobie.metadata as metadata
from mobie.metadata.source_metadata import _get_table_metadata
from mobie.xml_utils import add_s3_to_xml, _parse_s3_xml


def migrate_data_spec(dataset_folder):
    """ Update to the new imageData and tableData spec.

    See https://github.com/mobie/mobie.github.io/issues/49 for details.
    """
    dataset_metadata = metadata.read_dataset_metadata(dataset_folder)
    sources = dataset_metadata['sources']

    remote_folder = None
    endpoint, bucket, region = None, None, None

    new_sources = {}
    for source_name, source in sources.items():
        new_source = deepcopy(source)
        source_type = list(new_source.keys())[0]

        image_data = new_source[source_type].pop('imageDataLocations')
        relative_xml = image_data["fileSystem"]
        xml = os.path.join(dataset_folder, relative_xml)
        new_image_data = {
            "format": "bdv.n5", "relativePath": relative_xml
        }
        if "s3store" in image_data:
            remote_xml = os.path.join(dataset_folder, image_data["s3store"])
            remote_folder = os.path.split(remote_xml)[0]

            path_in_bucket, endpoint, bucket, region = _parse_s3_xml(remote_xml)

            # update the xml
            add_s3_to_xml(xml, path_in_bucket)

        new_source[source_type]["imageData"] = new_image_data

        if "tableDataLocation" in source[source_type]:
            table_data = new_source[source_type].pop("tableDataLocation")
            new_table_data = {
                "format": "tsv", "relativePath": table_data
            }
            new_source[source_type]['tableData'] = new_table_data

        new_sources[source_name] = new_source
    dataset_metadata['sources'] = new_sources

    if remote_folder is not None:
        # remove the folder with remote image data
        cmd = ['git', 'rm', '-r', remote_folder]
        run(cmd)

    # update the view spec
    views = dataset_metadata['views']
    new_views = {}
    for name, view in views.items():
        if metadata.is_grid_view(view):
            new_view = deepcopy(view)
            table_location = new_view["grid"].pop("tableDataLocation")
            new_view["grid"]["tableData"] = _get_table_metadata(table_location)
            new_views[name] = new_view
        else:
            new_views[name] = view
    dataset_metadata['views'] = new_views

    metadata.write_dataset_metadata(dataset_folder, dataset_metadata)
    return endpoint, bucket, region
