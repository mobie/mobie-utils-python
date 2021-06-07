from copy import deepcopy

import mobie.metadata as metadata
from mobie.metadata.source_metadata import _get_table_metadata


def migrate_data_spec(dataset_folder):
    """ Update to the new imageData and tableData spec.

    See https://github.com/mobie/mobie.github.io/issues/49 for details.
    """
    dataset_metadata = metadata.read_dataset_metadata(dataset_folder)
    sources = dataset_metadata['sources']

    file_formats = {"bdv.n5"}
    new_sources = {}
    for source_name, source in sources.items():
        new_source = deepcopy(source)
        source_type = list(new_source.keys())[0]

        image_data = new_source[source_type].pop('imageDataLocations')
        relative_xml = image_data["fileSystem"]
        new_image_data = {
            "bdv.n5": {"relativePath": relative_xml}
        }
        if "s3store" in image_data:
            new_image_data["bdv.n5.s3"] = {"relativePath": image_data["s3store"]}
            file_formats.add("bdv.n5.s3")

        new_source[source_type]["imageData"] = new_image_data

        if "tableDataLocation" in source[source_type]:
            table_location = new_source[source_type].pop("tableDataLocation")
            new_source[source_type]['tableData'] = _get_table_metadata(table_location)

        new_sources[source_name] = new_source
    dataset_metadata['sources'] = new_sources

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
    return list(file_formats)
