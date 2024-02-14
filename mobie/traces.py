import multiprocessing
import os

from pybdv.metadata import get_data_path

import mobie.metadata as metadata
import mobie.utils as utils
from mobie.import_data import import_traces
from mobie.tables import compute_trace_default_table


# TODO use a spot source underlying the traces instead of a segmentation source
# TODO make cluster tools task so this can be safely run on login nodes
def add_traces(input_folder, root, dataset_name, traces_name,
               reference_name, reference_scale,
               resolution, scale_factors, chunks,
               menu_name=None, file_format="bdv.n5", view=None,
               max_jobs=multiprocessing.cpu_count(),
               add_default_table=True,
               seg_infos={}, unit='micrometer',
               description=None):
    """ Add traces to an existing MoBIE dataset.

    Currently supports nmx and swc format.

    Arguments:
        input_folder [str] - input folder with trace files.
        root [str] - data root folder.
        dataset_name [str] - name of the dataset the segmentation should be added to.
        traces_name [str] - name of the segmentation.
        reference_name [str] - name of the reference data, from which the shape of the
            trace volume will be derived
        reference_scale [int] - scale level of the reference data to use
        resolution [list[float]] - resolution of the segmentation in micrometer.
        scale_factors [list[list[int]]] - scale factors used for down-sampling.
        menu_name [str] - menu item for this source.
            If none is given will be created based on the image name. (default: None)
        file_format [str] - the file format used to store the data internally (default: ome.zarr)
        view [dict] - default view settings for this source (default: None)
        chunks [list[int]] - chunks for the data.
        max_jobs [int] - number of jobs (default: number of cores)
        add_default_table [bool] - whether to add the default table (default: True)
        seg_infos [dict] - segmentation information that will be added to the table (default: {})
        unit [str] - physical unit of the coordinate system (default: micrometer)
        description [str] - description for the traces (default: None)
    """
    view = utils.require_dataset_and_view(root, dataset_name, file_format,
                                          source_type="segmentation",
                                          source_name=traces_name,
                                          menu_name=menu_name, view=view,
                                          is_default_dataset=False,
                                          description=description)

    # get the path to the reference data
    dataset_folder = os.path.join(root, dataset_name)
    # NOTE: we require that the reference data and traces are in the same file format
    # and that it is a file format that can be read locally, i.e. has 'relativePath'
    source_metadata = metadata.read_dataset_metadata(dataset_folder)['sources'][reference_name]
    source_metadata = source_metadata[list(source_metadata.keys())[0]]['imageData']
    assert file_format in source_metadata
    assert 'relativePath' in source_metadata[file_format]
    reference_path = os.path.join(dataset_folder, source_metadata[file_format]['relativePath'])
    if file_format.startswith('bdv'):
        reference_path = get_data_path(reference_path, return_absolute_path=True)

    data_path, image_metadata_path = utils.get_internal_paths(dataset_folder, file_format, traces_name)
    # import the segmentation data
    import_traces(input_folder, data_path,
                  reference_path, reference_scale,
                  resolution=resolution,
                  scale_factors=scale_factors,
                  chunks=chunks,
                  max_jobs=max_jobs,
                  unit=unit,
                  source_name=traces_name)

    # compute the default segmentation table
    if add_default_table:
        table_folder = os.path.join(dataset_folder, 'tables', traces_name)
        table_path = os.path.join(table_folder, 'default.tsv')
        os.makedirs(table_folder, exist_ok=True)
        compute_trace_default_table(input_folder, table_path, resolution,
                                    seg_infos=seg_infos)
    else:
        table_folder = None

    metadata.add_source_to_dataset(dataset_folder, 'segmentation',
                                   traces_name, image_metadata_path,
                                   view=view, table_folder=table_folder)


def main():
    parser = utils.get_base_parser("Add traces to MoBIE dataset")
    parser.add_argument('--reference_name', type=str,
                        help="name of the reference data volume",
                        required=True)
    parser.add_argument('--reference_scale', type=int, default=0,
                        help='scale level to consider for the reference data')
    parser.add_argument('--add_default_table', type=int, default=1,
                        help="whether to add the default table")

    args = parser.parse_args()

    resolution, scale_factors, chunks, transformation = utils.parse_spatial_args(args)
    view = utils.parse_view(args)

    if transformation is not None:
        raise NotImplementedError("Transformation is currently not supported")

    add_traces(args.input_path, args.root, args.dataset_name, args.name,
               args.reference_name, args.reference_scale,
               menu_name=args.menu_name, view=view,
               resolution=resolution, add_default_table=bool(args.add_default_table),
               scale_factors=scale_factors, chunks=chunks, max_jobs=args.max_jobs)
