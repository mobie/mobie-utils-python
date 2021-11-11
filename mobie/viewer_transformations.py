import elf.transformation as trafo
import numpy as np


def affine_to_position(affine, bdv_window_center=[400.0, 206.5, 0.0]):
    center = np.array(list(bdv_window_center) + [1.0])
    viewer_trafo = trafo.parameters_to_matrix(affine)
    return (np.linalg.inv(viewer_trafo) @ center)[:-1].tolist()


def normalized_affine_to_affine(normalized_affine, bdv_window_width=800.0, bdv_window_center=[400.0, 206.5, 0.0]):
    affine = trafo.parameters_to_matrix(normalized_affine)

    bdv_window_width = 800
    bdv_scale = 1. / bdv_window_width
    scale = trafo.affine_matrix_3d(scale=[bdv_scale, bdv_scale, bdv_scale])

    affine = np.linalg.inv(scale) @ affine

    bdv_window_center = [400, 206.5, 0.0]
    shift = trafo.affine_matrix_3d(translation=bdv_window_center)
    affine = shift @ affine

    return trafo.matrix_to_parameters(affine)


def normalized_affine_to_position(normalized_affine):
    affine = normalized_affine_to_affine(normalized_affine)
    position = affine_to_position(affine)
    return position
