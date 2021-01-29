from pybdv.metadata import write_affine


def update_transformation_parameter(xml_path, parameter):
    if len(parameter) != 12:
        raise ValueError("Expected affine transformation with 12 parameters, got {len(parameter)}")
    write_affine(xml_path, setup_id=0, affine=parameter,
                 overwrite=True, timepoint=0)
