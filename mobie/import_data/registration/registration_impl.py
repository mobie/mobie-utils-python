"""@private
"""
import functools
import os
import shutil
import subprocess
from glob import glob

import bioimage_py as bp
import numpy as np
from elf.transformation import (elastix_parser,
                                elastix_to_bdv,
                                elastix_to_native)
from mobie.xml_utils import copy_xml_with_relpath
from pybdv.metadata import write_affine

from mobie.utils import get_run_config
from mobie.import_data.utils import _create_level, _open_storage, _remove_output, _write_block_shape

# numpy/scipy interpolation order for the affine method.
_INTERPOLATION_TO_ORDER = {"nearest": 0, "linear": 1, "quadratic": 2, "cubic": 3}

# allowed transformix settings (previously enforced by cluster_tools' TransformixBase).
_TRANSFORMIX_FORMATS = ("bdv", "tif")
_TRANSFORMIX_INTERPOLATION = {"linear": "FinalLinearInterpolator",
                              "nearest": "FinalNearestNeighborInterpolator"}
_TRANSFORMIX_RESULT_TYPES = ("unsigned char", "unsigned short")


def determine_shape(transformation, resolution, scale_factor=1e3):
    shape = elastix_parser.get_shape(transformation)[::-1]

    resolution_elastix = elastix_parser.get_resolution(transformation)[::-1]
    resolution_elastix = [res * scale_factor for res in resolution_elastix]

    rescaling = [res_e / res for res_e, res in zip(resolution_elastix, resolution)]
    shape = tuple(int(sh * res) for sh, res in zip(shape, rescaling))
    return shape


def registration_affine(input_path, input_key,
                        output_path, output_key,
                        transformation, interpolation,
                        shape, resolution, chunks,
                        tmp_folder, target, max_jobs,
                        bounding_box=None, file_format="ome.zarr",
                        ome_zarr_version="0.4", shards=None):
    """Apply registration via bioimage-py's affine source wrapper.

    Only works for affine transformations. This replaces the former nifty-backed
    ``cluster_tools.transformations.AffineTransformationWorkflow``: the elastix transformation is
    converted to a native (numpy zyx, output->input) affine matrix, applied on read via an
    ``AffineSource`` and materialized into the scale-0 dataset with a block-wise ``bp.copy``.
    """
    # the native matrix is already in numpy (zyx) axis order and the output->input direction that
    # AffineSource expects, so it can be used directly (no axis flip / parameter conversion).
    matrix = elastix_to_native(transformation, resolution=resolution)

    if shape is None:
        shape = determine_shape(transformation, resolution)
    shape = tuple(int(s) for s in shape)

    if interpolation not in _INTERPOLATION_TO_ORDER:
        raise ValueError(f"Invalid interpolation mode {interpolation}")
    order = _INTERPOLATION_TO_ORDER[interpolation]

    src = bp.open_source(input_path, input_key) if input_key else bp.open_source(input_path)
    # nifty did not support pre-smoothing, so we keep anti-aliasing disabled to match the old result.
    affine = bp.wrapper.AffineSource(src, shape=shape, affine_matrix=matrix,
                                     order=order, fill_value=0, anti_aliasing=False)

    job_type, job_config, num_workers = get_run_config(target, max_jobs, tmp_folder)

    # re-import overwrites any previous conversion at the output location.
    _remove_output(output_path)
    with _open_storage(output_path, file_format, mode="a", ome_zarr_version=ome_zarr_version) as f:
        ds = _create_level(f, file_format, 0, shape, chunks, src.dtype, shards=shards)
        block_shape = _write_block_shape(ds)
        # restrict the computation to the bounding box (output space) when one is given. We derive
        # the block ids from the same full-shape blocking that bp.copy uses internally, so only
        # blocks overlapping the box are written (matching the old block-granular roi behavior).
        block_ids = None
        if bounding_box is not None:
            blocking = bp.util.get_blocking(shape, block_shape)
            begin = [0] * len(shape) if bounding_box[0] is None else [int(b) for b in bounding_box[0]]
            end = list(shape) if bounding_box[1] is None else [int(b) for b in bounding_box[1]]
            block_ids = blocking.get_block_ids_overlapping_bounding_box(begin, end)
        bp.copy(affine, output=ds, block_shape=block_shape, block_ids=block_ids,
                job_type=job_type, job_config=job_config, num_workers=num_workers)


def registration_bdv(input_path, output_path, transformation, resolution):
    """Apply registration by writing affine transformation to bdv.
    Only works for affine transformations.
    """
    assert input_path.endswith(".xml")
    assert output_path.endswith(".xml")

    trafo = elastix_to_bdv(transformation, resolution)
    # copy the xml path and replace the file path with the correct relative filepath
    copy_xml_with_relpath(input_path, output_path)

    # replace the affine trafo in the new xml file
    write_affine(output_path, setup_id=0, timepoint=0, affine=trafo, overwrite=True)


def _update_transformix_transformation(in_file, out_file, tmp_folder,
                                       interpolation, result_dtype, shape, resolution):
    """Rewrite a single elastix transformation file for transformix.

    Updates the interpolation mode, result pixel type, size and spacing, and rewrites the
    InitialTransformParametersFileName so the (also rewritten) chained transformations are found.
    """
    interpolator_name = _TRANSFORMIX_INTERPOLATION[interpolation]

    def update_line(line, to_write, is_numeric):
        line = line.rstrip("\n").split()
        if is_numeric:
            line = [line[0], "%s)" % to_write]
        else:
            line = [line[0], "\"%s\")" % to_write]
        return " ".join(line) + "\n"

    with open(in_file, "r") as f_in, open(out_file, "w") as f_out:
        for line in f_in:
            if line.startswith("(ResampleInterpolator"):
                line = update_line(line, interpolator_name, False)
            elif line.startswith("(ResultImagePixelType") and result_dtype is not None:
                line = update_line(line, result_dtype, False)
            elif line.startswith("(Size") and shape is not None:
                shape_str = " ".join(map(str, shape[::-1]))
                line = update_line(line, shape_str, True)
            elif line.startswith("(Spacing") and resolution is not None:
                resolution_str = " ".join(map(str, resolution[::-1]))
                line = update_line(line, resolution_str, True)
            elif line.startswith("(InitialTransformParametersFileName"):
                initial_trafo_file = line.split()[-1][1:-2]
                if initial_trafo_file == "NoInitialTransform":
                    continue
                new_initial_trafo_file = os.path.join(tmp_folder, "transformations",
                                                      os.path.split(initial_trafo_file)[1])
                line = update_line(line, new_initial_trafo_file, False)
            f_out.write(line)


def _update_transformix_transformations(transformation, tmp_folder,
                                        interpolation, result_dtype, shape, resolution):
    """Rewrite all elastix transformation files in the transformation folder, return the new path."""
    trafo_folder, trafo_name = os.path.split(transformation)
    out_folder = os.path.join(tmp_folder, "transformations")
    os.makedirs(out_folder, exist_ok=True)

    for trafo in glob(os.path.join(trafo_folder, "*.txt")):
        name = os.path.split(trafo)[1]
        _update_transformix_transformation(trafo, os.path.join(out_folder, name), tmp_folder,
                                           interpolation, result_dtype, shape, resolution)

    new_trafo = os.path.join(out_folder, trafo_name)
    assert os.path.exists(new_trafo)
    return new_trafo


def _apply_transformix_for_file(input_path, output_path, transformation_file,
                                fiji_executable, elastix_directory, working_dir,
                                n_threads, output_format):
    """Run the fiji elastixWrapper 'Transformix' plugin on a single input file."""
    assert os.path.exists(elastix_directory)
    assert os.path.exists(working_dir)
    assert os.path.exists(input_path)
    assert os.path.exists(transformation_file)

    if output_format == "tif":
        format_str = "Save as Tiff"
    elif output_format == "bdv":
        format_str = "Save as BigDataViewer .xml/.h5"
    else:
        raise ValueError(f"Invalid output format {output_format}")

    trafo_dir, trafo_name = os.path.split(transformation_file)
    # transformix arguments are passed as a single comma-separated string.
    transformix_argument = ["elastixDirectory=\'%s\'" % elastix_directory,
                            "workingDirectory=\'%s\'" % os.path.abspath(working_dir),
                            "inputImageFile=\'%s\'" % os.path.abspath(input_path),
                            "transformationFile=\'%s\'" % trafo_name,
                            "outputFile=\'%s\'" % os.path.abspath(output_path),
                            "outputModality=\'%s\'" % format_str,
                            "numThreads=\'%i\'" % n_threads]
    transformix_argument = ",".join(transformix_argument)
    transformix_argument = "\"%s\"" % transformix_argument
    cmd = [fiji_executable, "--ij2", "--headless", "--run", "\"Transformix\"", transformix_argument]
    cmd_str = " ".join(cmd)

    cwd = os.getcwd()
    try:
        # change into the transformation directory so relative paths in the transformations resolve.
        os.chdir(trafo_dir)
        # the CLI parser only works when the whole command is passed as one string with shell=True.
        subprocess.check_output([cmd_str], shell=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(e.output.decode("utf-8"))
    finally:
        os.chdir(cwd)

    # the elastix plugin can fail without a non-zero exit code, so we check for the expected output.
    expected_output = output_path + "-ch0.tif" if output_format == "tif" else output_path + ".xml"
    assert os.path.exists(expected_output), f"The output {expected_output} is not there."


def _run_transformix_file(index, input_paths, output_paths, transformation_file,
                          fiji_executable, elastix_directory, tmp_folder,
                          output_format, n_threads):
    """Per-item worker for the runner: apply transformix to a single input/output pair."""
    working_dir = os.path.join(tmp_folder, "work_dir%i" % index)
    os.makedirs(working_dir, exist_ok=True)
    _apply_transformix_for_file(input_paths[index], output_paths[index], transformation_file,
                                fiji_executable, elastix_directory, working_dir,
                                n_threads, output_format)


def registration_transformix(input_path, output_path,
                             transformation, fiji_executable,
                             elastix_directory, tmp_folder,
                             shape, resolution,
                             interpolation="nearest", output_format="tif",
                             result_dtype="unsigned char",
                             n_threads=8, target="local", max_jobs=1):
    """Apply registration by using transformix from the fiji elastix wrapper.

    The fiji subprocess is dispatched through the bioimage-py runner (one task per input file),
    replacing the former ``cluster_tools.transformations.TransformixTransformationWorkflow``.
    """
    if result_dtype not in _TRANSFORMIX_RESULT_TYPES:
        raise ValueError(f"Expected result_dtype to be one of {_TRANSFORMIX_RESULT_TYPES}, got {result_dtype}")
    if interpolation not in _TRANSFORMIX_INTERPOLATION:
        raise ValueError(f"Expected interpolation to be one of {tuple(_TRANSFORMIX_INTERPOLATION)}, "
                         f"got {interpolation}")
    if output_format not in _TRANSFORMIX_FORMATS:
        raise ValueError(f"Expected output_format to be one of {_TRANSFORMIX_FORMATS}, got {output_format}")

    if shape is None:
        shape = determine_shape(transformation, resolution)

    trafo_file = _update_transformix_transformations(transformation, tmp_folder,
                                                     interpolation, result_dtype, shape, resolution)

    inputs = [os.path.abspath(input_path)]
    outputs = [os.path.abspath(output_path)]

    job_type, job_config, num_workers = get_run_config(target, max_jobs, tmp_folder)
    runner = bp.get_runner(job_type, job_config)
    runner.map(
        functools.partial(_run_transformix_file,
                          input_paths=inputs, output_paths=outputs,
                          transformation_file=trafo_file,
                          fiji_executable=fiji_executable,
                          elastix_directory=elastix_directory,
                          tmp_folder=tmp_folder,
                          output_format=output_format,
                          n_threads=n_threads),
        len(inputs), num_workers=num_workers, has_return_val=False, name="transformix",
    )


def _write_coords(begin, end, out_file):
    """Write a block's output voxel coordinates as transformix index points.

    Matches the cluster_tools format: header ``index`` + point count, then one ``x y z`` line per
    voxel iterating z-outer to x-inner (numpy C-order), so the transformix output points come back
    in the block's C-order.
    """
    n_coords = (end[0] - begin[0]) * (end[1] - begin[1]) * (end[2] - begin[2])
    with open(out_file, "w") as f:
        f.write("index\n")
        f.write(f"{n_coords}\n")
        for z in range(begin[0], end[0]):
            for y in range(begin[1], end[1]):
                for x in range(begin[2], end[2]):
                    f.write(f"{x} {y} {z}\n")


def _parse_outputpoints(out_file, ndim, block_shape):
    """Parse transformix ``outputpoints.txt`` into a ``(ndim, *block_shape)`` source coordinate field.

    Replicates nifty's convention: the source voxel index to sample is the ``OutputIndexFixed`` field,
    read in transformix xyz order and reversed to numpy zyx. The points are in the C-order in which
    the input points were written, so they reshape directly onto the block.
    """
    key = "OutputIndexFixed = [ "
    coords = []
    with open(out_file) as f:
        for line in f:
            start = line.find(key)
            if start == -1:
                continue
            start += len(key)
            stop = line.find("]", start)
            values = [int(v) for v in line[start:stop].split()]
            coords.append(values[::-1])  # transformix xyz -> numpy zyx
    coords = np.array(coords, dtype="float64")
    n_expected = int(np.prod(block_shape))
    if coords.shape != (n_expected, ndim):
        raise RuntimeError(
            f"Expected {n_expected} transformix output points of dimension {ndim}, got {coords.shape}"
        )
    # (n_points, ndim) in C-order -> (ndim, *block_shape)
    return np.ascontiguousarray(coords.T.reshape((ndim,) + tuple(block_shape)))


def _update_coordinate_transformation(in_file, out_file, tmp_folder, resolution):
    """Rewrite a single elastix transformation file for the transformix coordinate (`-def`) mode."""
    def update_line(line, to_write, is_numeric):
        line = line.rstrip("\n").split()
        if is_numeric:
            line = [line[0], "%s)" % to_write]
        else:
            line = [line[0], "\"%s\")" % to_write]
        return " ".join(line) + "\n"

    with open(in_file, "r") as f_in, open(out_file, "w") as f_out:
        for line in f_in:
            if line.startswith("(Spacing") and resolution is not None:
                resolution_str = " ".join(map(str, resolution[::-1]))
                line = update_line(line, resolution_str, True)
            elif line.startswith("(InitialTransformParametersFileName"):
                initial_trafo_file = line.split()[-1][1:-2]
                if initial_trafo_file == "NoInitialTransform":
                    continue
                new_initial_trafo_file = os.path.join(tmp_folder, "transformations",
                                                      os.path.split(initial_trafo_file)[1])
                line = update_line(line, new_initial_trafo_file, False)
            f_out.write(line)


def _update_coordinate_transformations(transformation, tmp_folder, resolution):
    """Rewrite all elastix transformation files in the transformation folder, return the new path."""
    trafo_folder, trafo_name = os.path.split(transformation)
    out_folder = os.path.join(tmp_folder, "transformations")
    os.makedirs(out_folder, exist_ok=True)
    for trafo in glob(os.path.join(trafo_folder, "*.txt")):
        name = os.path.split(trafo)[1]
        _update_coordinate_transformation(trafo, os.path.join(out_folder, name), tmp_folder, resolution)
    new_trafo = os.path.join(out_folder, trafo_name)
    assert os.path.exists(new_trafo)
    return new_trafo


def _run_transformix_coordinates(in_coord_file, coord_folder, transformation_file, elastix_directory):
    """Run the elastix transformix binary in coordinate (`-def`) mode for one block."""
    transformix_bin = os.path.join(elastix_directory, "bin", "transformix")
    env = dict(os.environ)
    elastix_lib = os.path.join(elastix_directory, "lib")
    env["LD_LIBRARY_PATH"] = elastix_lib + os.pathsep + env.get("LD_LIBRARY_PATH", "")
    cmd = [transformix_bin, "-def", in_coord_file, "-out", coord_folder, "-tp", transformation_file]
    subprocess.run(cmd, env=env)
    out_file = os.path.join(coord_folder, "outputpoints.txt")
    if not os.path.exists(out_file):
        raise RuntimeError(f"transformix did not produce the expected output {out_file}")
    return out_file


def _coordinate_worker(block_id, input_path, input_key, output_path, output_key,
                       transformation_file, elastix_directory, tmp_folder,
                       shape, block_shape, order, file_format, ome_zarr_version="0.4"):
    """Per-block worker: transformix-map the block's output coordinates and resample the source."""
    blocking = bp.util.get_blocking(shape, block_shape)
    block = blocking.get_block(block_id)
    begin = [int(b) for b in block.begin]
    end = [int(e) for e in block.end]

    coord_folder = os.path.join(tmp_folder, f"coords_{block_id}")
    os.makedirs(coord_folder, exist_ok=True)
    try:
        in_coord_file = os.path.join(coord_folder, "inpoints.txt")
        _write_coords(begin, end, in_coord_file)
        out_file = _run_transformix_coordinates(in_coord_file, coord_folder,
                                                transformation_file, elastix_directory)

        block_shape_local = tuple(e - b for b, e in zip(begin, end))
        coords = _parse_outputpoints(out_file, len(shape), block_shape_local)

        source = bp.open_source(input_path, input_key) if input_key else bp.open_source(input_path)
        out_block = bp.transformation.transform_subvolume_coordinates(
            source, coords, order=order, fill_value=0,
        )

        bb = tuple(slice(b, e) for b, e in zip(begin, end))
        with _open_storage(output_path, file_format, mode="a", ome_zarr_version=ome_zarr_version) as f:
            f[output_key][bb] = out_block
    finally:
        shutil.rmtree(coord_folder, ignore_errors=True)


def registration_coordinate(input_path, input_key,
                            output_path, output_key,
                            transformation, elastix_directory,
                            shape, resolution, chunks,
                            tmp_folder, target, max_jobs,
                            interpolation="linear", bounding_box=None,
                            file_format="ome.zarr",
                            ome_zarr_version="0.4", shards=None):
    """Apply registration via transformix coordinate transformation, resampled with map_coordinates.

    For each output block the block's voxel coordinates are mapped into the source image by the elastix
    ``transformix`` binary (``-def`` mode); the mapped source coordinates are then resampled with
    ``bioimage_cpp.transformation.map_coordinates`` (via bioimage-py's ``transform_subvolume_coordinates``).
    The per-block work is dispatched through the bioimage-py runner, replacing the former cluster_tools /
    nifty ``coordinateTransformationZ5`` path.
    """
    if file_format in ("bdv", "bdv.hdf5") and target == "slurm":
        raise ValueError(
            "The bdv.hdf5 format does not support distributed (slurm) writing. "
            "Use target='local' or a different file format."
        )
    if shards is not None and not (file_format == "ome.zarr" and ome_zarr_version == "0.5"):
        raise ValueError("Sharding is only supported for the ome.zarr v0.5 (zarr v3) format.")

    if shape is None:
        shape = determine_shape(transformation, resolution)
    shape = tuple(int(s) for s in shape)

    if interpolation not in _INTERPOLATION_TO_ORDER:
        raise ValueError(f"Invalid interpolation mode {interpolation}")
    order = _INTERPOLATION_TO_ORDER[interpolation]

    trafo_file = _update_coordinate_transformations(transformation, tmp_folder, resolution)

    src = bp.open_source(input_path, input_key) if input_key else bp.open_source(input_path)
    dtype = src.dtype

    # re-import overwrites any previous conversion at the output location, then create scale 0.
    _remove_output(output_path)
    with _open_storage(output_path, file_format, mode="a", ome_zarr_version=ome_zarr_version) as f:
        ds = _create_level(f, file_format, 0, shape, chunks, dtype, shards=shards)
        # for a sharded array many chunks share one shard file, so write at shard granularity.
        block_shape = _write_block_shape(ds)

    blocking = bp.util.get_blocking(shape, block_shape)
    if bounding_box is None:
        n_blocks = blocking.number_of_blocks
        item_ids = None
    else:
        begin = [0] * len(shape) if bounding_box[0] is None else [int(b) for b in bounding_box[0]]
        end = list(shape) if bounding_box[1] is None else [int(b) for b in bounding_box[1]]
        item_ids = blocking.get_block_ids_overlapping_bounding_box(begin, end)
        n_blocks = None

    job_type, job_config, num_workers = get_run_config(target, max_jobs, tmp_folder)
    runner = bp.get_runner(job_type, job_config)
    runner.map(
        functools.partial(_coordinate_worker,
                          input_path=input_path, input_key=input_key,
                          output_path=output_path, output_key=output_key,
                          transformation_file=trafo_file,
                          elastix_directory=elastix_directory,
                          tmp_folder=tmp_folder,
                          shape=shape, block_shape=block_shape,
                          order=order, file_format=file_format,
                          ome_zarr_version=ome_zarr_version),
        n_blocks, item_ids=item_ids, num_workers=num_workers,
        has_return_val=False, name="transformix-coordinate",
    )
