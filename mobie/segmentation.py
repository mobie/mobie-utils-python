import multiprocessing
import os

from pybdv.metadata import get_key
import mobie.metadata as metadata
from mobie.import_data import (import_segmentation,
                               import_segmentation_from_node_labels,
                               import_segmentation_from_paintera,
                               is_paintera)
from mobie.tables import compute_default_table
from mobie.utils import get_base_parser, parse_spatial_args, parse_view
from mobie.validation import validate_view_metadata


# TODO support transformation
# TODO support default arguments for scale factors and chunks
def add_segmentation(input_path, input_key,
                     root, dataset_name, segmentation_name,
                     resolution, scale_factors, chunks,
                     menu_name=None,
                     node_label_path=None, node_label_key=None,
                     tmp_folder=None, target='local',
                     max_jobs=multiprocessing.cpu_count(),
                     add_default_table=True, view=None,
                     postprocess_config=None, unit='micrometer'):
    """ Add segmentation source to MoBIE dataset.

    Arguments:
        input_path [str] - path to the segmentation to add.
        input_key [str] - key to the segmentation to add.
        root [str] - data root folder.
        dataset_name [str] - name of the dataset the segmentation should be added to.
        segmentation_name [str] - name of the segmentation.
        resolution [list[float]] - resolution of the segmentation in micrometer.
        scale_factors [list[list[int]]] - scale factors used for down-sampling.
        chunks [list[int]] - chunks for the data.
        menu_name [str] - menu name for this source.
            If none is given will be created based on the image name. (default: None)
        node_label_path [str] - path to node labels (default: None)
        node_label_key [str] - key to node labels (default: None)
        tmp_folder [str] - folder for temporary files (default: None)
        target [str] - computation target (default: 'local')
        max_jobs [int] - number of jobs (default: number of cores)
        add_default_table [bool] - whether to add the default table (default: True)
        view [dict] - default view settings for this source (default: None)
        postprocess_config [dict] - config for postprocessing,
            only available for paintera dataset (default: None)
        unit [str] - physical unit of the coordinate system (default: micrometer)
    """
    # check that we have this dataset
    if not metadata.dataset_exists(root, dataset_name):
        raise ValueError(f"Dataset {dataset_name} not found in {root}")

    if view is None:
        view = metadata.get_default_view("segmentation", segmentation_name, menu_name=menu_name)
    elif view is not None and menu_name is not None:
        view.update({"uiSelectionGroup": menu_name})
    validate_view_metadata(view, sources=[segmentation_name])

    tmp_folder = f'tmp_{dataset_name}_{segmentation_name}' if tmp_folder is None else tmp_folder

    # import the segmentation data
    dataset_folder = os.path.join(root, dataset_name)
    data_path = os.path.join(dataset_folder, 'images', f'{segmentation_name}.n5')
    xml_path = os.path.join(dataset_folder, 'images', f'{segmentation_name}.xml')
    if node_label_path is not None:
        if node_label_key is None:
            raise ValueError("Expect node_label_key if node_label_path is given")
        import_segmentation_from_node_labels(input_path, input_key, data_path,
                                             node_label_path, node_label_key,
                                             resolution, scale_factors, chunks,
                                             tmp_folder=tmp_folder, target=target,
                                             max_jobs=max_jobs, unit=unit,
                                             source_name=segmentation_name)
    elif is_paintera(input_path, input_key):
        import_segmentation_from_paintera(input_path, input_key, data_path,
                                          resolution, scale_factors, chunks,
                                          tmp_folder=tmp_folder, target=target,
                                          max_jobs=max_jobs,
                                          postprocess_config=postprocess_config,
                                          unit=unit, source_name=segmentation_name)
    else:
        import_segmentation(input_path, input_key, data_path,
                            resolution, scale_factors, chunks,
                            tmp_folder=tmp_folder, target=target,
                            max_jobs=max_jobs, unit=unit,
                            source_name=segmentation_name)

    # compute the default segmentation table
    if add_default_table:
        table_folder = os.path.join(dataset_folder, 'tables', segmentation_name)
        table_path = os.path.join(table_folder, 'default.tsv')
        os.makedirs(table_folder, exist_ok=True)
        key = get_key(False, 0, 0, 0)
        compute_default_table(data_path, key, table_path, resolution,
                              tmp_folder=tmp_folder, target=target,
                              max_jobs=max_jobs)
    else:
        table_folder = None

    # add the segmentation to the image dict
    metadata.add_source_metadata(dataset_folder, 'segmentation',
                                 segmentation_name, xml_path,
                                 table_folder=table_folder, view=view)


def main():
    description = "Add segmentation source to MoBIE dataset."
    parser = get_base_parser(description)
    parser.add_argument('--node_label_path', type=str, default=None,
                        help="path to the node_labels for the segmentation")
    parser.add_argument('--node_label_key', type=str, default=None,
                        help="key for the node labels for segmentation")
    parser.add_argument('--add_default_table', type=int, default=1,
                        help="whether to add the default table")
    args = parser.parse_args()

    resolution, scale_factors, chunks, transformation = parse_spatial_args(args)
    view = parse_view(args)

    if transformation is not None:
        raise NotImplementedError("Transformation is currently not supported")

    add_segmentation(args.input_path, args.input_key,
                     args.root, args.dataset_name, args.name,
                     node_label_path=args.node_label_path, node_label_key=args.node_label_key,
                     resolution=resolution, scale_factors=scale_factors, chunks=chunks,
                     add_default_table=bool(args.add_default_table),
                     view=view, unit=args.unit, menu_name=args.menu_name,
                     tmp_folder=args.tmp_folder, target=args.target, max_jobs=args.max_jobs)
