"""Helper functions for converting viewer transformations to global coordinate transformations.
"""
from typing import List, Union

import elf.transformation as trafo
import numpy as np


def affine_to_position(
    affine: Union[np.ndarray, List[float]],
    bdv_window_center: List[float] = [400.0, 206.5, 0.0],
) -> np.ndarray:
    """Extract the position transformation / shift from an affine transformation.

    Args:
        affine: The affine transformation.
        bdv_window_center: The BDV window center.

    Returns:
        The affine transformation encoding only the position.
    """
    center = np.array(list(bdv_window_center) + [1.0])
    viewer_trafo = trafo.parameters_to_matrix(affine)
    return (np.linalg.inv(viewer_trafo) @ center)[:-1].tolist()


def normalized_affine_to_affine(
    normalized_affine: List[float],
    bdv_window_width: float = 800.0,
    bdv_window_center: List[float] = [400.0, 206.5, 0.0],
) -> List[float]:
    """Convert a normalized viewer transform to an affine transformation in the coordinate system.

    Args:
        normalized_affine: The normalized affine transformation.
        bdv_window_width: The width of the BDV window.
        bdv_window_center: The BDV window center.

    Returns:
        The affine transformation in the global coordinate system.
    """
    affine = trafo.parameters_to_matrix(normalized_affine)

    bdv_window_width = 800
    bdv_scale = 1. / bdv_window_width
    scale = trafo.affine_matrix_3d(scale=[bdv_scale, bdv_scale, bdv_scale])

    affine = np.linalg.inv(scale) @ affine

    bdv_window_center = [400, 206.5, 0.0]
    shift = trafo.affine_matrix_3d(translation=bdv_window_center)
    affine = shift @ affine

    return trafo.matrix_to_parameters(affine)


def normalized_affine_to_position(normalized_affine: List[float]) -> np.ndarray:
    """Extract the position transformation / shift from a normalized affine transformation.

    Args:
        affine: The normalized affine transformation.

    Returns:
        The affine transformation encoding only the position.
    """
    affine = normalized_affine_to_affine(normalized_affine)
    position = affine_to_position(affine)
    return position
