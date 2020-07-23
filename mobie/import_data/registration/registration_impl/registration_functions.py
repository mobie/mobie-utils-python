import json
import os

import luigi
from elf.transformation.elastix_parser import get_bdv_affine_transformation
from mobie.xml_utils import copy_xml_with_relpath
from pybdv.metadata import write_affine
from .transformix_registration import TransformixRegistrationLocal, TransformixRegistrationSlurm


def registration_affine(input_path, input_key,
                        output_path, output_key,
                        transformation_file):
    """Apply registration by using elf/nifty affine transormation function.
    Only works for affine transformations.
    """


def registration_bdv(input_path, output_path, transformation_file):
    """Apply registration by writing affine transformation to bdv.
    Only works for affine transformations.
    """
    assert input_path.endswith('.xml')
    assert output_path.endswith('.xml')

    # get the transformation in bdv format
    trafo = get_bdv_affine_transformation(transformation_file)

    # copy the xml path and replace the file path with the correct relative filepath
    copy_xml_with_relpath(input_path, output_path)

    # replace the affine trafo in the new xml file
    write_affine(output_path, setup_id=0, timepoint=0, affine=trafo, overwrite=True)


def registration_transformix(input_path, output_path,
                             transformation_file, fiji_executable,
                             elastix_directory, tmp_folder,
                             interpolation='nearest', output_format='tif',
                             result_dtype='unsigned char',
                             n_threads=8, target='local'):
    """Apply registration by using tranformix from the fiji elastix wrapper.
    """
    task = TransformixRegistrationSlurm if target == 'slurm' else TransformixRegistrationLocal
    if result_dtype not in task.result_types:
        raise ValueError(f"Expected result_dtype to be one of {task.result_types}, got {result_dtype}")
    if interpolation not in task.interpolation_modes:
        raise ValueError(f"Expected interpolation to be one of {task.interpolation_modes}, got {interpolation}")
    if output_format not in task.formats:
        raise ValueError(f"Expected output_format to be one of {task.formats}, got {output_format}")

    config_dir = os.path.join(tmp_folder, 'configs')
    os.makedirs(config_dir, exist_ok=True)

    task_config = task.default_task_config()
    task_config.update({'mem_limit': 16, 'time_limit': 240, 'threads_per_job': n_threads,
                        'ResultImagePixelType': result_dtype})
    with open(os.path.join(config_dir, 'apply_registration.config'), 'w') as f:
        json.dump(task_config, f)

    in_file = os.path.join(tmp_folder, 'inputs.json')
    with open(in_file, 'w') as f:
        json.dump([input_path], f)

    out_file = os.path.join(tmp_folder, 'outputs.json')
    with open(out_file, 'w') as f:
        json.dump([output_path], f)

    t = task(tmp_folder=tmp_folder, config_dir=config_dir, max_jobs=1,
             input_path_file=in_file, output_path_file=out_file,
             fiji_executable=fiji_executable, elastix_directory=elastix_directory,
             transformation_file=transformation_file, output_format=output_format,
             interpolation=interpolation)
    ret = luigi.build([t], local_scheduler=True)
    if not ret:
        raise RuntimeError("Apply registration failed")
