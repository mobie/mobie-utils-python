import multiprocessing
import os

from pybdv.metadata import get_data_path

import mobie.metadata as metadata
from mobie.import_data import import_traces
from mobie.tables import compute_trace_default_table
from mobie.utils import get_base_parser, parse_spatial_args, parse_view
from mobie.validation import validate_view_metadata


# TODO make cluster tools task so this can be safely run on login nodes
def add_traces(input_folder, root, dataset_name, traces_name,
               reference_name, reference_scale,
               resolution, scale_factors, chunks,
               menu_name=None, view=None,
               max_jobs=multiprocessing.cpu_count(),
               add_default_table=True,
               seg_infos={}, unit='micrometer'):
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
        view [dict] - default view settings for this source (default: None)
        chunks [list[int]] - chunks for the data.
        max_jobs [int] - number of jobs (default: number of cores)
        add_default_table [bool] - whether to add the default table (default: True)
        seg_infos [dict] - segmentation information that will be added to the table (default: {})
        unit [str] - physical unit of the coordinate system (default: micrometer)
    """
    # check that we have this dataset
    if not metadata.dataset_exists(root, dataset_name):
        raise ValueError(f"Dataset {dataset_name} not found in {root}")

    if view is None:
        view = metadata.get_default_view("segmentation", traces_name, menu_name=menu_name)
    elif view is not None and menu_name is not None:
        view.update({"uiSelectionGroup": menu_name})
    validate_view_metadata(view, sources=[traces_name])

    dataset_folder = os.path.join(root, dataset_name)
    # get the path to the reference data
    reference_xml = os.path.join(dataset_folder, 'images', f'{reference_name}.xml')
    reference_path = get_data_path(reference_xml, return_absolute_path=True)

    # import the segmentation data
    data_path = os.path.join(dataset_folder, 'images', f'{traces_name}.n5')
    xml_path = os.path.join(dataset_folder, 'images', f'{traces_name}.xml')
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

    metadata.add_source_metadata(dataset_folder, 'segmentation',
                                 traces_name, xml_path,
                                 view=view, table_folder=table_folder)


def main():
    parser = get_base_parser("Add traces to MoBIE dataset")
    parser.add_argument('--reference_name', type=str,
                        help="name of the reference data volume",
                        required=True)
    parser.add_argument('--reference_scale', type=int, default=0,
                        help='scale level to consider for the reference data')
    parser.add_argument('--add_default_table', type=int, default=1,
                        help="whether to add the default table")

    args = parser.parse_args()

    resolution, scale_factors, chunks, transformation = parse_spatial_args(args)
    view = parse_view(args)

    if transformation is not None:
        raise NotImplementedError("Transformation is currently not supported")

    add_traces(args.input_path, args.root, args.dataset_name, args.name,
               args.reference_name, args.reference_scale,
               menu_name=args.menu_name, view=view,
               resolution=resolution, add_default_table=bool(args.add_default_table),
               scale_factors=scale_factors, chunks=chunks, max_jobs=args.max_jobs)
