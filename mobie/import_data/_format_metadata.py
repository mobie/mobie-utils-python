"""@private

Write the multiscale image-data metadata for the supported MoBIE storage formats.

The pyramid *data* is written by the import layer (via bioimage-py); this module only writes
the format-specific *metadata*: the ome.zarr NGFF "multiscales" attributes, or the bdv.n5 /
bdv.hdf5 internal attributes plus the companion bdv xml (both via pybdv). The data datasets
must already exist on disk when these functions are called.

This functionality used to live in cluster_tools (`utils.volume_utils.write_format_metadata`);
it now lives here because, per the migration design, MoBIE-specific format-metadata writing
belongs in mobie, not in bioimage-py.
"""
import os

import z5py
from pybdv.metadata import write_h5_metadata, write_n5_metadata, write_xml_metadata
from pybdv.util import relative_to_absolute_scale_factors

# NGFF axis name -> axis type
AXES_TYPES = {"t": "time", "c": "channel", "z": "space", "y": "space", "x": "space"}


def _write_ome_zarr_metadata(path, metadata_dict, scale_factors):
    setup_name = metadata_dict.get("setup_name", None)
    setup_name = "data" if setup_name is None else setup_name
    unit = metadata_dict.get("unit", "pixel")

    with z5py.File(path, mode="a", dimension_separator="/") as f:
        ndim = f["s0"].ndim
        axes_names = ["y", "x"] if ndim == 2 else ["z", "y", "x"]
        resolution = metadata_dict.get("resolution", [1.0] * ndim)

        # the scale factors are relative per-level; prepend the identity for s0 and
        # accumulate to absolute factors, then scale by the physical resolution.
        abs_factors = relative_to_absolute_scale_factors([[1] * ndim] + list(scale_factors))
        scales = [[float(sf * res) for sf, res in zip(factor, resolution)] for factor in abs_factors]

        axes = [{"name": name, "type": AXES_TYPES[name], "unit": unit} for name in axes_names]
        datasets = [
            {"path": f"s{level}", "coordinateTransformations": [{"type": "scale", "scale": scale}]}
            for level, scale in enumerate(scales)
        ]
        f.attrs["multiscales"] = [
            {"axes": axes, "datasets": datasets, "name": setup_name, "version": "0.4"}
        ]


def _write_bdv_metadata(metadata_format, path, metadata_dict, scale_factors):
    is_h5 = metadata_format in ("bdv", "bdv.hdf5")
    xml_path = os.path.splitext(path)[0] + ".xml"

    # bdv expects the relative scale factors including the identity for s0.
    scale_factors = [[1, 1, 1]] + list(scale_factors)
    unit = metadata_dict.get("unit", "pixel")
    resolution = metadata_dict.get("resolution", [1.0, 1.0, 1.0])
    setup_name = metadata_dict.get("setup_name", None)

    write_xml_metadata(xml_path, path, unit, resolution, is_h5,
                       setup_id=0, timepoint=0, setup_name=setup_name, affine=None,
                       attributes={"channel": {"id": 0}}, overwrite=False,
                       overwrite_data=False, enforce_consistency=False)
    if is_h5:
        write_h5_metadata(path, scale_factors)
    else:
        write_n5_metadata(path, scale_factors, resolution)


def write_format_metadata(metadata_format, path, metadata_dict, scale_factors):
    """Write the multiscale metadata for the given storage format.

    Args:
        metadata_format: The storage format. One of 'ome.zarr', 'bdv.n5', 'bdv.hdf5'.
        path: The path to the (already written) multiscale data.
        metadata_dict: The metadata values, with keys 'resolution', 'unit' and 'setup_name'.
        scale_factors: The relative per-level downscaling factors (without the s0 identity).

    Raises:
        ValueError: If the storage format is not supported.
    """
    if metadata_format == "ome.zarr":
        _write_ome_zarr_metadata(path, metadata_dict, scale_factors)
    elif metadata_format in ("bdv", "bdv.n5", "bdv.hdf5"):
        _write_bdv_metadata(metadata_format, path, metadata_dict, scale_factors)
    else:
        raise ValueError(f"Unsupported metadata format: {metadata_format}")
