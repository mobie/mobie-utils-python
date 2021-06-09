from copy import deepcopy
import mobie.metadata as metadata
from mobie.validation import validate_dataset


def migrate_table_spec(dataset_folder):
    """ Require all table names in view tables field and add tables field to grid view.

    See https://github.com/mobie/mobie.github.io/issues/54 for details.
    """
    dataset_metadata = metadata.read_dataset_metadata(dataset_folder)

    # still wrong in some datasets
    if 'is2d' in dataset_metadata:
        dataset_metadata['is2D'] = dataset_metadata.pop('is2d')

    sources = dataset_metadata['sources']
    views = dataset_metadata['views']
    new_views = {}

    for name, view in views.items():
        new_view = deepcopy(view)

        # do we have a segmentation display? -> update 'tables'
        if 'sourceDisplays' in view:
            for ii, disp in enumerate(view['sourceDisplays']):
                for disp_name, disp_vals in disp.items():
                    if 'tables' in disp_vals:
                        tables = ['default.tsv'] + disp_vals['tables']
                        new_view['sourceDisplays'][ii][disp_name]['tables'] = tables
                    else:
                        source = sources[disp_vals['sources'][0]]
                        has_tables = 'segmentation' in source and 'tableData' in source['segmentation']
                        if has_tables:
                            tables = ['default.tsv']
                            new_view['sourceDisplays'][ii][disp_name]['tables'] = tables

        # do we have a grid transform? -> add tables
        if 'sourceTransforms' in view:
            for ii, trafo in enumerate(view['sourceTransforms']):
                for trafo_name, trafo_vals in trafo.items():
                    if trafo_name == 'grid':
                        new_view['sourceTransforms'][ii][trafo_name]['tables'] = ['default.tsv']

        new_views[name] = new_view

    dataset_metadata['views'] = new_views
    metadata.write_dataset_metadata(dataset_folder, dataset_metadata)
    validate_dataset(dataset_folder)
