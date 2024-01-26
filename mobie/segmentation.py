import multiprocessing
import os

import mobie
import numpy as np
import pandas as pd

from mobie.import_data import (import_segmentation,
                               import_segmentation_from_node_labels,
                               import_segmentation_from_paintera,
                               is_paintera)
from mobie.tables import check_and_copy_default_table, compute_default_table


# TODO support transformation
# TODO support default arguments for scale factors and chunks
def add_segmentation(input_path, input_key,
                     root, dataset_name, segmentation_name,
                     resolution, scale_factors, chunks,
                     menu_name=None, file_format="bdv.n5",
                     node_label_path=None, node_label_key=None,
                     tmp_folder=None, target="local",
                     max_jobs=multiprocessing.cpu_count(),
                     add_default_table=True, view=None,
                     postprocess_config=None, unit="micrometer",
                     is_default_dataset=False, description=None):
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
        file_format [str] - the file format used to store the data internally (default: bdv.n5)
        node_label_path [str] - path to node labels (default: None)
        node_label_key [str] - key to node labels (default: None)
        tmp_folder [str] - folder for temporary files (default: None)
        target [str] - computation target (default: "local")
        max_jobs [int] - number of jobs (default: number of cores)
        add_default_table [bool, str, pd.DataFrame] - whether to add the default table.
            Can also be a filepath to a table or a pandas DataFrame.
            In the two latter cases the default table will be initialized from the passed data. (default: True)
        view [dict] - default view settings for this source (default: None)
        postprocess_config [dict] - config for postprocessing,
            only available for paintera dataset (default: None)
        unit [str] - physical unit of the coordinate system (default: micrometer)
        is_default_dataset [bool] - whether to set new dataset as default dataset.
            Only applies if the dataset is being created. (default: False)
        description [str] - description for this segmentation (default: None)
    """
    if isinstance(input_path, np.ndarray):
        input_path, input_key = mobie.utils.save_temp_input(input_path, tmp_folder, segmentation_name)

    view = mobie.utils.require_dataset_and_view(root, dataset_name, file_format,
                                                source_type="segmentation",
                                                source_name=segmentation_name,
                                                menu_name=menu_name, view=view,
                                                is_default_dataset=is_default_dataset,
                                                description=description)

    dataset_folder = os.path.join(root, dataset_name)
    tmp_folder = f"tmp_{dataset_name}_{segmentation_name}" if tmp_folder is None else tmp_folder

    # import the segmentation data
    data_path, image_metadata_path = mobie.utils.get_internal_paths(dataset_folder, file_format,
                                                                    segmentation_name)
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
        if file_format != "bdv.n5":
            raise NotImplementedError
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
                            source_name=segmentation_name,
                            file_format=file_format)

    # we initialize with an already computed default table
    if isinstance(add_default_table, (str, pd.DataFrame)):
        table_folder = os.path.join(dataset_folder, "tables", segmentation_name)
        table_path = os.path.join(table_folder, "default.tsv")
        os.makedirs(table_folder, exist_ok=True)
        input_table = add_default_table
        is_2d = mobie.metadata.read_dataset_metadata(dataset_folder).get("is2D", False)
        check_and_copy_default_table(input_table, table_path, is_2d)
    # compute the default segmentation table
    elif add_default_table:
        table_folder = os.path.join(dataset_folder, "tables", segmentation_name)
        table_path = os.path.join(table_folder, "default.tsv")
        os.makedirs(table_folder, exist_ok=True)
        key = mobie.utils.get_data_key(file_format, scale=0, path=data_path)
        compute_default_table(data_path, key, table_path, resolution,
                              tmp_folder=tmp_folder, target=target,
                              max_jobs=max_jobs)
    else:
        table_folder = None

    # add the segmentation to the dataset metadata
    mobie.metadata.add_source_to_dataset(dataset_folder, "segmentation",
                                         segmentation_name, image_metadata_path,
                                         table_folder=table_folder, view=view)


def main():
    description = "Add segmentation source to MoBIE dataset."
    parser = mobie.utils.get_base_parser(description)
    parser.add_argument("--node_label_path", type=str, default=None,
                        help="path to the node_labels for the segmentation")
    parser.add_argument("--node_label_key", type=str, default=None,
                        help="key for the node labels for segmentation")
    parser.add_argument("--add_default_table", type=int, default=1,
                        help="whether to add the default table")
    args = parser.parse_args()

    resolution, scale_factors, chunks, transformation = mobie.utils.parse_spatial_args(args)
    view = mobie.utils.parse_view(args)

    if transformation is not None:
        raise NotImplementedError("Transformation is currently not supported")

    add_segmentation(args.input_path, args.input_key,
                     args.root, args.dataset_name, args.name,
                     node_label_path=args.node_label_path, node_label_key=args.node_label_key,
                     resolution=resolution, scale_factors=scale_factors, chunks=chunks,
                     add_default_table=bool(args.add_default_table),
                     view=view, unit=args.unit, menu_name=args.menu_name,
                     tmp_folder=args.tmp_folder, target=args.target, max_jobs=args.max_jobs,
                     is_default_dataset=bool(args.is_default_dataset))
