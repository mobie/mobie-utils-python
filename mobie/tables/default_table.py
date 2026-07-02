"""Functionality for creating segmentation tables.
"""
import os
import warnings
from typing import Sequence

import bioimage_py as bp
import pandas as pd

from .utils import read_table
from ..utils import get_run_config
from ..validation.tables import get_columns_for_table_format


def check_and_copy_default_table(input_path, output_path, is_2d, suppress_warnings=False):
    """@private
    """
    tab = read_table(input_path)
    required_column_names, recommended_column_names, _ = get_columns_for_table_format(tab, is_2d)
    missing_columns = list(required_column_names - set(tab.columns))
    if missing_columns:
        raise ValueError(f"The table at {input_path} is missing the following required columns: {missing_columns}")
    missing_columns = list(recommended_column_names - set(tab.columns))
    if missing_columns and not suppress_warnings:
        warnings.warn(f"The table at {input_path} is missing the following recommended columns: {missing_columns}")
    tab.to_csv(output_path, sep="\t", index=False, na_rep="nan")


def _output_columns(ndim):
    """@private
    The MoBIE default-table columns, in the conventional order: xyz for 3d, yx for 2d."""
    if ndim == 2:
        return ["label_id", "anchor_y", "anchor_x",
                "bb_min_y", "bb_min_x", "bb_max_y", "bb_max_x", "n_pixels"]
    return ["label_id",
            "anchor_x", "anchor_y", "anchor_z",
            "bb_min_x", "bb_min_y", "bb_min_z",
            "bb_max_x", "bb_max_y", "bb_max_z",
            "n_pixels"]


def _compute_table_df(src, resolution, run_kwargs, correct_anchors):
    """@private
    Compute the MoBIE default table for a segmentation source via bioimage-py.

    The per-label base statistics (size, center of mass, bounding box) come from
    `bp.morphology.morphology`. If `correct_anchors` is set, a second (more expensive) pass via
    `bp.morphology.regionprops` moves the anchor inside the object (the center of mass when it lies
    inside, otherwise the deepest-interior voxel); otherwise the center of mass is used directly.
    Works for 2d and 3d.
    """
    ndim = src.ndim
    axes = ["y", "x"] if ndim == 2 else ["z", "y", "x"]
    resolution = [float(r) for r in resolution]
    assert len(resolution) == ndim, f"{len(resolution)}, {ndim}"

    # base morphology (size, com, bbox); always required.
    morph = bp.morphology.morphology(src, **run_kwargs)

    columns = _output_columns(ndim)
    if len(morph) == 0:
        return pd.DataFrame(columns=columns)

    if correct_anchors:
        # second pass to move the anchor inside the object; the centroid is already in physical
        # units and regionprops is sorted by label like morph, so the rows align positionally.
        props = bp.morphology.regionprops(src, morph, resolution=resolution, **run_kwargs)
        anchors = {ax: props[f"centroid_{ax}"].values for ax in axes}
    else:
        # cheaper: the center of mass (in voxels) scaled to physical units.
        anchors = {ax: morph[f"com_{ax}"].values * res for ax, res in zip(axes, resolution)}

    data = {"label_id": morph["label"].values, "n_pixels": morph["size"].values}
    for ax, res in zip(axes, resolution):
        data[f"anchor_{ax}"] = anchors[ax]
        data[f"bb_min_{ax}"] = morph[f"bb_min_{ax}"].values * res
        # morphology writes bb_max as the exclusive slice stop; subtract 1 to recover the
        # inclusive-max convention (matching the previous cluster_tools / nifty output).
        data[f"bb_max_{ax}"] = (morph[f"bb_max_{ax}"].values - 1) * res

    return pd.DataFrame(data)[columns]


def compute_default_table(
    seg_path: str,
    seg_key: str,
    table_path: str,
    resolution: Sequence[float],
    tmp_folder: str,
    target: str,
    max_jobs: int,
    correct_anchors: bool = True,
) -> None:
    """Compute the default table for a segmentation, containing the attributes required to view it in MoBIE.

    Args:
        seg_path: The input path to the segmentation.
        seg_key: The key to the segmenation.
        table_path: The path to the output table.
        resolution: The resolution of the data in physical units.
        tmp_folder: The folder for temporary files.
        target: The computation target.
        max_jobs: The number of jobs for parallelization.
        correct_anchors: Whether to move the anchor points into the segmentation objects, so that
            concave objects do not get an anchor outside of their boundaries (via
            `bioimage_py.morphology.regionprops`). This is an additional, relatively expensive pass,
            so it can be deactivated; the (cheaper) center of mass is then used as the anchor.
    """
    src = bp.open_source(seg_path, seg_key) if seg_key else bp.open_source(seg_path)

    if src.ndim == 2:
        # 2d data is small; compute it in-memory. Parallelization over many 2d images (e.g. for
        # high-throughput microscopy) is the caller's responsibility, not bioimage-py's block runner.
        run_kwargs = dict(job_type="local", num_workers=1)
    else:
        job_type, job_config, num_workers = get_run_config(target, max_jobs, tmp_folder)
        run_kwargs = dict(job_type=job_type, job_config=job_config, num_workers=num_workers)

    table = _compute_table_df(src, resolution, run_kwargs, correct_anchors)

    # write output to csv
    table_folder = os.path.split(table_path)[0]
    os.makedirs(table_folder, exist_ok=True)
    table.to_csv(table_path, sep="\t", index=False, na_rep="nan")
