import argparse
import json
import multiprocessing
import os

from pybdv.metadata import get_data_path

from mobie.import_data import import_traces
from mobie.metadata import add_to_image_dict, have_dataset
from mobie.tables import compute_trace_default_table


# TODO make cluster tools task so this can be safely run on login nodes
def add_traces(input_folder, root, dataset_name, traces_name,
               reference_name, reference_scale,
               resolution, scale_factors, chunks,
               max_jobs=multiprocessing.cpu_count(),
               add_default_table=True, settings=None,
               seg_infos={}):
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
        chunks [list[int]] - chunks for the data.
        max_jobs [int] - number of jobs (default: number of cores)
        add_default_table [bool] - whether to add the default table (default: True)
        settings [dict] - layer settings for the segmentation (default: None)
        seg_infos [dict] - segmentation information that will be added to the table (default: {})
    """
    # check that we have this dataset
    if not have_dataset(root, dataset_name):
        raise ValueError(f"Dataset {dataset_name} not found in {root}")

    dataset_folder = os.path.join(root, dataset_name)
    # get the path to the reference data
    reference_xml = os.path.join(dataset_folder, 'images', 'local', f'{reference_name}.xml')
    reference_path = get_data_path(reference_xml, return_absolute_path=True)

    # import the segmentation data
    data_path = os.path.join(dataset_folder, 'images', 'local', f'{traces_name}.n5')
    xml_path = os.path.join(dataset_folder, 'images', 'local', f'{traces_name}.xml')
    import_traces(input_folder, data_path,
                  reference_path, reference_scale,
                  resolution=resolution,
                  scale_factors=scale_factors,
                  chunks=chunks,
                  max_jobs=max_jobs)

    # compute the default segmentation table
    if add_default_table:
        table_folder = os.path.join(dataset_folder, 'tables', traces_name)
        table_path = os.path.join(table_folder, 'default.csv')
        os.makedirs(table_folder, exist_ok=True)
        compute_trace_default_table(input_folder, table_path, resolution,
                                    seg_infos=seg_infos)
    else:
        table_folder = None

    # add the segmentation to the image dict
    add_to_image_dict(dataset_folder, 'segmentation', xml_path,
                      table_folder=table_folder)


def main():
    parser = argparse.ArgumentParser(description="Add traces to MoBIE dataset")
    parser.add_argument('--input_folder', type=str,
                        help="path to the folder with traces",
                        required=True)
    parser.add_argument('--root', type=str,
                        help="root folder of the MoBIE project",
                        required=True)
    parser.add_argument('--dataset_name', type=str,
                        help="name of the dataset to which the traces are added",
                        required=True)
    parser.add_argument('--traces_name', type=str,
                        help="name of the traces to be added",
                        required=True)
    parser.add_argument('--reference_name', type=str,
                        help="name of the reference data volume",
                        required=True)

    parser.add_argument('--resolution', type=str,
                        help="resolution of the traces in micrometer, json-encoded",
                        required=True)
    parser.add_argument('--scale_factors', type=str,
                        help="factors used for downscaling the data, json-encoded",
                        required=True)
    parser.add_argument('--chunks', type=str,
                        help="chunks of the data that is added, json-encoded",
                        required=True)

    parser.add_argument('--reference_scale', type=int, default=0,
                        help='scale level to consider for the reference data')
    parser.add_argument('--add_default_table', type=int, default=1,
                        help="whether to add the default table")
    parser.add_argument('--max_jobs', type=int, default=multiprocessing.cpu_count(),
                        help="number of jobs")

    args = parser.parse_args()

    # resolution, scale_factors and chunks need to be json encoded
    resolution = json.loads(args.resolution)
    scale_factors = json.loads(args.scale_factors)
    chunks = json.loads(args.chunks)

    add_traces(args.input_folder, args.root, args.dataset_name, args.traces_name,
               args.reference_name, args.reference_scale,
               resolution=resolution, add_default_table=bool(args.add_default_table),
               scale_factors=scale_factors, chunks=chunks, max_jobs=args.max_jobs)
