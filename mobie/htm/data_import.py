import multiprocessing
import os

import luigi
from cluster_tools.copy_sources import get_copy_task

from .. import metadata
from .. import utils


def _copy_image_data(files, key, root,
                     dataset_name, source_names,
                     file_format,  resolution, unit,
                     scale_factors, chunks,
                     tmp_folder, target, max_jobs):
    ds_folder = os.path.join(root, dataset_name)
    sources = list(metadata.read_dataset_metadata(ds_folder).get("sources", {}).keys())

    # don't copy sources that are already present
    input_names = list(set(source_names) - set(sources))
    if not input_names:
        return [], []

    input_files = [files[input_names.index(name)] for name in input_names]
    out_paths = [utils.get_internal_paths(ds_folder, file_format, name)
                 for name in input_names]
    output_files = [paths[0] for paths in out_paths]
    metadata_paths = [paths[1] for paths in out_paths]

    task = get_copy_task(target)
    config_dir = os.path.join(tmp_folder, "configs")
    image_metadata = {"resolution": resolution, "unit": unit}

    t = task(input_files=input_files, output_files=output_files, key=key,
             metadata_format=file_format, scale_factors=scale_factors, chunks=chunks,
             metadata_dict=image_metadata, names=input_names,
             tmp_folder=tmp_folder, max_jobs=max_jobs, config_dir=config_dir)
    assert luigi.build([t], local_scheduler=True), "Copying the sources failed"
    return input_names, metadata_paths


def _require_dataset(root, dataset_name, file_format, is_default_dataset):
    ds_exists = utils.require_dataset(root, dataset_name, file_format)
    dataset_folder = os.path.join(root, dataset_name)
    if not ds_exists:
        metadata.create_dataset_structure(root, dataset_name, [file_format])
        metadata.create_dataset_metadata(dataset_folder)
        metadata.add_dataset(root, dataset_name, is_default_dataset)


def _add_tables(file_format, paths):
    pass


def _add_sources(dataset_folder, source_names, paths, file_format, source_type, table_folders=None):
    assert len(source_names) == len(paths)
    if table_folders is None:
        table_folders = len(source_names) * [None]
    for name, metadata_path, table_folder in zip(source_names, paths, table_folders):
        metadata.add_source_to_dataset(dataset_folder, source_type, name, metadata_path,
                                       table_folder=table_folder, view={})


def add_images(files, root,
               dataset_name, image_names,
               resolution, scale_factors, chunks,
               key=None, file_format="bdv.n5",
               tmp_folder=None, target="local", max_jobs=multiprocessing.cpu_count(),
               unit="micrometer", is_default_dataset=False):
    assert len(files) == len(image_names)

    # require the dataset
    _require_dataset(root, dataset_name, file_format, is_default_dataset)
    tmp_folder = f"tmp_{dataset_name}_{image_names[0]}" if tmp_folder is None else tmp_folder

    # copy all the image data into the dataset with the given file format
    source_names, metadata_paths = _copy_image_data(files, key, root,
                                                    dataset_name, image_names,
                                                    file_format,  resolution, unit,
                                                    scale_factors, chunks,
                                                    tmp_folder, target, max_jobs)

    # add metadata for all the images
    if source_names:
        _add_sources(os.path.join(root, dataset_name), source_names, metadata_paths,
                     file_format, "image")


def add_segmentations(files, root,
                      dataset_name, segmentation_names,
                      resolution, scale_factors, chunks,
                      key=None, file_format="bdv.n5",
                      tmp_folder=None, target="local", max_jobs=multiprocessing.cpu_count(),
                      add_default_tables=True, unit="micrometer", is_default_dataset=False):
    assert len(files) == len(segmentation_names)

    # require the dataset
    _require_dataset(root, dataset_name, file_format, is_default_dataset)
    tmp_folder = f"tmp_{dataset_name}_{segmentation_names[0]}" if tmp_folder is None else tmp_folder

    # copy all the image data into the dataset with the given file format
    source_names, metadata_paths = _copy_image_data(files, key, root,
                                                    dataset_name, segmentation_names,
                                                    file_format,  resolution, unit,
                                                    scale_factors, chunks,
                                                    tmp_folder, target, max_jobs)

    if add_default_tables:
        table_folders = _add_tables(file_format, metadata_paths)
    else:
        table_folders = None

    # add metadata for all the images
    if source_names:
        _add_sources(os.path.join(root, dataset_name), source_names, metadata_paths,
                     file_format, "segmentation", table_folders)
