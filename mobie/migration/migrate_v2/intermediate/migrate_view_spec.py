from copy import deepcopy
import mobie.metadata as metadata


def migrate_view_spec(dataset_folder):
    """ Move the "default" view of a source from nested inside the source
    to dataset.json:views.

    See https://github.com/mobie/mobie.github.io/issues/46 for details.
    """
    dataset_metadata = metadata.read_dataset_metadata(dataset_folder)
    sources = dataset_metadata['sources']
    views = dataset_metadata['views']

    new_sources = {}
    for source_name, source in sources.items():
        new_source = deepcopy(source)

        source_type = list(new_source.keys())[0]
        view = new_source[source_type].pop('view')

        new_sources[source_name] = new_source
        views[source_name] = view

    dataset_metadata['sources'] = new_sources
    dataset_metadata['views'] = views

    metadata.write_dataset_metadata(dataset_folder, dataset_metadata)
