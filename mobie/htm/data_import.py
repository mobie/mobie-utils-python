"""Functionality for creating sources from high content microscopy data.
"""
import functools
import multiprocessing
import os
from typing import List, Optional, Sequence

import bioimage_py as bp

from .. import metadata
from .. import utils
from ..import_data import import_image_data, import_segmentation
from ..tables import compute_default_table


def _import_one_source(index, input_files, output_files, names, key, file_format,
                       resolution, unit, scale_factors, chunks, is_seg,
                       ome_zarr_version="0.4", shards=None):
    # Each source is converted in-memory (target "local"); parallelization happens over
    # sources in `_copy_image_data`, not via bioimage-py's within-source block runner.
    import_fn = import_segmentation if is_seg else import_image_data
    import_fn(
        input_files[index], key, output_files[index],
        resolution, scale_factors, chunks,
        tmp_folder=None, target="local", max_jobs=1,
        unit=unit, source_name=names[index], file_format=file_format,
        ome_zarr_version=ome_zarr_version, shards=shards,
    )


def _copy_image_data(files, key, root,
                     dataset_name, source_names,
                     file_format,  resolution, unit,
                     scale_factors, chunks,
                     tmp_folder, target, max_jobs, is_seg=False,
                     ome_zarr_version="0.4", shards=None):
    assert len(files) == len(source_names)
    ds_folder = os.path.join(root, dataset_name)
    sources = list(metadata.read_dataset_metadata(ds_folder).get("sources", {}).keys())

    # don't copy sources that are already present
    input_names = list(set(source_names) - set(sources))
    if not input_names:
        return [], []

    input_files = [files[source_names.index(name)] for name in input_names]
    out_paths = [utils.get_internal_paths(ds_folder, file_format, name)
                 for name in input_names]
    output_files = [paths[0] for paths in out_paths]
    metadata_paths = [paths[1] for paths in out_paths]

    # import each source into the dataset, parallelizing over sources (one task per source).
    job_type, job_config, num_workers = utils.get_run_config(target, max_jobs, tmp_folder)
    runner = bp.get_runner(job_type, job_config)
    runner.map(
        functools.partial(_import_one_source, input_files=input_files, output_files=output_files,
                          names=input_names, key=key, file_format=file_format,
                          resolution=resolution, unit=unit, scale_factors=scale_factors,
                          chunks=chunks, is_seg=is_seg,
                          ome_zarr_version=ome_zarr_version, shards=shards),
        len(input_files), num_workers=num_workers, has_return_val=False, name="htm-import",
    )
    return input_names, metadata_paths


def _require_dataset(root, dataset_name, file_format, is_default_dataset, is2d):
    ds_exists = utils.require_dataset(root, dataset_name)
    dataset_folder = os.path.join(root, dataset_name)
    if not ds_exists:
        metadata.create_dataset_structure(root, dataset_name, [file_format])
        metadata.create_dataset_metadata(dataset_folder, is2d=is2d)
        metadata.add_dataset(root, dataset_name, is_default_dataset)


def _compute_one_table(index, input_files, table_paths, input_key, resolution):
    # Each image is computed in-memory (target "local"); the parallelization happens over images
    # in `_add_tables`, not via bioimage-py's within-image block runner. The anchors are always
    # corrected so they fall inside the (potentially concave) objects.
    compute_default_table(
        input_files[index], input_key, table_paths[index], resolution,
        tmp_folder=None, target="local", max_jobs=1, correct_anchors=True,
    )


def _add_tables(file_format, paths,
                source_names, resolution,
                ds_folder, tmp_folder, target, max_jobs):
    table_folders = [os.path.join(ds_folder, "tables", name) for name in source_names]
    table_paths = [os.path.join(tab_folder, "default.tsv") for tab_folder in table_folders]
    input_key = utils.get_data_key(file_format, scale=0, path=paths[0])

    # parallelize the table computation over the images (one task per image).
    job_type, job_config, num_workers = utils.get_run_config(target, max_jobs, tmp_folder)
    runner = bp.get_runner(job_type, job_config)
    runner.map(
        functools.partial(_compute_one_table, input_files=paths, table_paths=table_paths,
                          input_key=input_key, resolution=resolution),
        len(paths), num_workers=num_workers, has_return_val=False, name="htm-tables",
    )

    return table_folders


def _add_sources(dataset_folder, source_names, paths,
                 file_format, source_type, table_folders=None):
    assert len(source_names) == len(paths)
    if table_folders is None:
        table_folders = len(source_names) * [None]
    for name, metadata_path, table_folder in zip(source_names, paths, table_folders):
        fname = os.path.split(metadata_path)[1].split(".")[0]
        assert fname == name, f"{fname}, {name}"
        if table_folder is not None:
            tname = os.path.split(table_folder)[1]
            assert tname == name, f"{tname}, {name}"
        metadata.add_source_to_dataset(dataset_folder, source_type, name, metadata_path,
                                       table_folder=table_folder, view={})


def add_images(
    files: Sequence[str],
    root: str,
    dataset_name: str,
    image_names: Sequence[str],
    resolution: Sequence[float],
    scale_factors: List[List[int]],
    chunks: Sequence[int],
    key: Optional[str] = None,
    file_format: str = "ome.zarr",
    tmp_folder: Optional[str] = None,
    target: str = "local",
    max_jobs: int = multiprocessing.cpu_count(),
    unit: str = "micrometer",
    is_default_dataset: bool = False,
    is2d: Optional[bool] = None,
    shards: Optional[Sequence[int]] = None,
) -> None:
    """Add images from a high-content microscopy experiment to a MoBIE dataset.

    Args:
        files: The input image files.
        root: The root directory of the MoBIE project.
        dataset_name: The name of the MoBIE dataset.
        image_names: The names of the images.
        resolution: The resolution in physical units.
        scale_factors: The scale factors to use for downsampling the data.
        chunks: The chunk size for the internal data format.
        key: The key / internal file path for the input image data.
            This is only required for hdf5, n5, zarr data; but not for tif or simlar.
        file_format: The internal file format to use.
        tmp_folder: The temporary folder for data conversion.
        target: The computational target.
        max_jobs: The number of jobs for parallelization.
        unit: The physical unit of the coordinate system.
        is_default_dataset: Whether this is the default dataset.
            Only relevant if the dataset will be created.
        is2d: Whether this is a 2D datasets.
        shards: The shard shape for zarr v3 sharding. Only supported for the ome.zarr v0.5 format
            (pass file_format='ome.zarr@0.5').
    """
    assert len(files) == len(image_names), f"{len(files)}, {len(image_names)}"

    # the ome.zarr / NGFF version may be encoded as a suffix on the file format (e.g. 'ome.zarr@0.5').
    file_format, ome_zarr_version = utils.parse_file_format(file_format)
    utils.check_shards(shards, file_format, ome_zarr_version)

    # require the dataset
    if is2d is None:
        is2d = len(resolution) == 2
    _require_dataset(root, dataset_name, file_format, is_default_dataset, is2d=is2d)
    tmp_folder = f"tmp_{dataset_name}_{image_names[0]}" if tmp_folder is None else tmp_folder

    # copy all the image data into the dataset with the given file format
    source_names, metadata_paths = _copy_image_data(files, key, root,
                                                    dataset_name, image_names,
                                                    file_format,  resolution, unit,
                                                    scale_factors, chunks,
                                                    tmp_folder, target, max_jobs, is_seg=False,
                                                    ome_zarr_version=ome_zarr_version, shards=shards)

    # add metadata for all the images
    if source_names:
        _add_sources(os.path.join(root, dataset_name), source_names, metadata_paths, file_format, "image")


def add_segmentations(
    files: Sequence[str],
    root: str,
    dataset_name: str,
    segmentation_names: Sequence[str],
    resolution: Sequence[float],
    scale_factors: List[List[int]],
    chunks: Sequence[int],
    key: Optional[str] = None,
    file_format: str = "ome.zarr",
    tmp_folder: Optional[str] = None,
    target: str = "local",
    max_jobs: int = multiprocessing.cpu_count(),
    add_default_tables: bool = True,
    unit: str = "micrometer",
    is_default_dataset: bool = False,
    is2d: Optional[bool] = None,
    shards: Optional[Sequence[int]] = None,
) -> None:
    """Add segmentation data for a high-content microscopy experiment to a MoBIE dataset.

    Args:
        files: The input segmentation files.
        root: The root directory of the MoBIE project.
        dataset_name: The name of the MoBIE dataset.
        segmentation_names: The names of the segmentations.
        resolution: The resolution in physical units.
        scale_factors: The scale factors to use for downsampling the data.
        chunks: The chunk size for the internal data format.
        key: The key / internal file path for the input image data.
            This is only required for hdf5, n5, zarr data; but not for tif or simlar.
        file_format: The internal file format to use.
        tmp_folder: The temporary folder for data conversion.
        target: The computational target.
        max_jobs: The number of jobs for parallelization.
        add_default_tables: Whether to create the default segmentation tables.
        unit: The physical unit of the coordinate system.
        is_default_dataset: Whether this is the default dataset.
            Only relevant if the dataset will be created.
        is2d: Whether this is a 2D datasets.
        shards: The shard shape for zarr v3 sharding. Only supported for the ome.zarr v0.5 format
            (pass file_format='ome.zarr@0.5').
    """
    assert len(files) == len(segmentation_names)

    # the ome.zarr / NGFF version may be encoded as a suffix on the file format (e.g. 'ome.zarr@0.5').
    file_format, ome_zarr_version = utils.parse_file_format(file_format)
    utils.check_shards(shards, file_format, ome_zarr_version)

    # require the dataset
    if is2d is None:
        is2d = len(resolution) == 2
    _require_dataset(root, dataset_name, file_format, is_default_dataset, is2d=is2d)
    tmp_folder = f"tmp_{dataset_name}_{segmentation_names[0]}" if tmp_folder\
        is None else tmp_folder

    # copy all the segmentation data into the dataset with the given file format
    source_names, metadata_paths = _copy_image_data(files, key, root,
                                                    dataset_name, segmentation_names,
                                                    file_format,  resolution, unit,
                                                    scale_factors, chunks,
                                                    tmp_folder, target, max_jobs, is_seg=True,
                                                    ome_zarr_version=ome_zarr_version, shards=shards)

    if add_default_tables:
        table_folders = _add_tables(file_format, metadata_paths,
                                    source_names, resolution,
                                    os.path.join(root, dataset_name),
                                    tmp_folder, target, max_jobs)
    else:
        table_folders = None

    # add metadata for all the images
    if source_names:
        _add_sources(
            os.path.join(root, dataset_name), source_names, metadata_paths, file_format, "segmentation", table_folders
        )
