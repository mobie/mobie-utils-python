import json
import os

import luigi

from elf.transformation import (elastix_parser,
                                elastix_to_bdv,
                                elastix_to_native,
                                matrix_to_parameters)
from mobie.xml_utils import copy_xml_with_relpath
from pybdv.metadata import write_affine


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
                        tmp_folder, target, max_jobs):
    """Apply registration by using elf/nifty affine transormation function.
    Only works for affine transformations.
    """
    from cluster_tools.transformations import AffineTransformationWorkflow
    task = AffineTransformationWorkflow
    config_dir = os.path.join(tmp_folder, 'configs')

    # load the transformation in bdv format
    # either join all transformation or implement chained application on the fly
    trafo = elastix_to_native(transformation, resolution=resolution)
    trafo = matrix_to_parameters(trafo)

    if shape is None:
        shape = determine_shape(transformation, resolution)

    # nifty does not support pre-smoothing yet, so we set sigma to None for all orders
    # once we support pre-smooting in nifty, should set it to 1 for everything but linear
    sigma = None
    # determine appropriate values for interpolation and sigma (anti-aliasing) based on interpolation
    if interpolation == 'nearest':
        order = 0
        # sigma= None
    elif interpolation == 'linear':
        order = 1
        # sigma = 1.
    elif interpolation == 'quadratic':
        order = 2
        # sigma = 1.
    elif interpolation == 'cubic':
        order = 3
        # sigma = 1.
    else:
        raise ValueError(f"Invalid interpolation mode {interpolation}")

    config = task.get_config()['affine']
    config.update({'chunks': chunks, 'sigma_anti_aliasing': sigma})

    with open(os.path.join(config_dir, 'affine.config'), 'w') as f:
        json.dump(config, f)

    t = task(tmp_folder=tmp_folder, config_dir=config_dir,
             target=target, max_jobs=max_jobs,
             input_path=input_path, input_key=input_key,
             output_path=output_path, output_key=output_key,
             transformation=trafo, shape=shape, order=order)
    ret = luigi.build([t], local_scheduler=True)
    if not ret:
        raise RuntimeError("Affine transformation failed")


def registration_bdv(input_path, output_path, transformation, resolution):
    """Apply registration by writing affine transformation to bdv.
    Only works for affine transformations.
    """
    assert input_path.endswith('.xml')
    assert output_path.endswith('.xml')

    trafo = elastix_to_bdv(transformation, resolution)
    # copy the xml path and replace the file path with the correct relative filepath
    copy_xml_with_relpath(input_path, output_path)

    # replace the affine trafo in the new xml file
    write_affine(output_path, setup_id=0, timepoint=0, affine=trafo, overwrite=True)


def registration_transformix(input_path, output_path,
                             transformation, fiji_executable,
                             elastix_directory, tmp_folder,
                             shape, resolution,
                             interpolation='nearest', output_format='tif',
                             result_dtype='unsigned char',
                             n_threads=8, target='local'):
    """Apply registration by using tranformix from the fiji elastix wrapper.
    """
    from cluster_tools.transformations import TransformixTransformationWorkflow
    task = TransformixTransformationWorkflow
    if result_dtype not in task.result_types:
        raise ValueError(f"Expected result_dtype to be one of {task.result_types}, got {result_dtype}")
    if interpolation not in task.interpolation_modes:
        raise ValueError(f"Expected interpolation to be one of {task.interpolation_modes}, got {interpolation}")
    if output_format not in task.formats:
        raise ValueError(f"Expected output_format to be one of {task.formats}, got {output_format}")

    config_dir = os.path.join(tmp_folder, 'configs')

    task_config = task.get_config()['transformix']
    task_config.update({'mem_limit': 16, 'time_limit': 240, 'threads_per_job': n_threads,
                        'ResultImagePixelType': result_dtype})
    with open(os.path.join(config_dir, 'transformix.config'), 'w') as f:
        json.dump(task_config, f)

    in_file = os.path.join(tmp_folder, 'inputs.json')
    with open(in_file, 'w') as f:
        json.dump([os.path.abspath(input_path)], f)

    out_file = os.path.join(tmp_folder, 'outputs.json')
    with open(out_file, 'w') as f:
        json.dump([os.path.abspath(output_path)], f)

    if shape is None:
        shape = determine_shape(transformation, resolution)

    t = task(tmp_folder=tmp_folder, config_dir=config_dir,
             max_jobs=1, target=target,
             input_path_file=in_file, output_path_file=out_file,
             fiji_executable=fiji_executable, elastix_directory=elastix_directory,
             transformation_file=transformation, output_format=output_format,
             interpolation=interpolation, shape=shape, resolution=resolution)
    ret = luigi.build([t], local_scheduler=True)
    if not ret:
        raise RuntimeError("Apply registration failed")


def registration_coordinate(input_path, input_key,
                            output_path, output_key,
                            transformation, elastix_directory,
                            shape, resolution,
                            tmp_folder, target, max_jobs):
    """Apply registration by using tranformix from the fiji elastix wrapper.
    """
    from cluster_tools.transformations import TransformixCoordinateTransformationWorkflow
    task = TransformixCoordinateTransformationWorkflow

    config_dir = os.path.join(tmp_folder, 'configs')

    task_config = task.get_config()['transformix_coordinate']
    task_config.update({'mem_limit': 8, 'time_limit': 240})
    with open(os.path.join(config_dir, 'transformix_coordinate.config'), 'w') as f:
        json.dump(task_config, f)

    if shape is None:
        shape = determine_shape(transformation, resolution)

    t = task(tmp_folder=tmp_folder, config_dir=config_dir,
             max_jobs=max_jobs, target=target,
             input_path=input_path, input_key=input_key,
             output_path=output_path, output_key=output_key,
             elastix_directory=elastix_directory,
             transformation_file=transformation,
             shape=shape, resolution=resolution)
    ret = luigi.build([t], local_scheduler=True)
    if not ret:
        raise RuntimeError("Apply registration failed")
