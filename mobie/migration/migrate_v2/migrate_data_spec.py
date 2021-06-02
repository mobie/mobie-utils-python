from copy import deepcopy
import mobie.metadata as metadata


def migrate_data_spec(dataset_folder):
    """ Update to the new imageData and tableData spec.

    See https://github.com/mobie/mobie.github.io/issues/49 for details.
    """
    dataset_metadata = metadata.read_dataset_metadata(dataset_folder)
    sources = dataset_metadata['sources']

    new_sources = {}
    for source_name, source in sources.items():
        new_source = deepcopy(source)
        source_type = list(new_source.keys())[0]

        image_data = new_source[source_type].pop('imageDataLocations')
        new_image_data = {
            "fileSystem": {"format": "bdv.n5", "source": image_data["fileSystem"]}
        }
        if "s3store" in image_data:
            have_remote_data = True
            new_image_data["gitHub"] = {"format": "bdv.n5.s3", "source": image_data["s3store"]}
        else:
            have_remote_data = False
        new_source[source_type]["imageData"] = new_image_data

        if "tableDataLocation" in source[source_type]:
            table_data = new_source[source_type].pop("tableDataLocation")
            new_table_data = {
                "fileSystem": {"format": "tsv", "source": table_data}
            }
            if have_remote_data:
                new_table_data["github"] = {"format": "tsv", "source": table_data}
            new_source[source_type]['tableData'] = new_table_data

        new_sources[source_name] = new_source

    dataset_metadata['sources'] = new_sources
    metadata.write_dataset_metadata(dataset_folder, dataset_metadata)
