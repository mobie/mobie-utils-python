import multiprocessing as mp
from .utils import downscale, ensure_volume


def import_image_data(in_path, in_key, out_path,
                      resolution, scale_factors, chunks,
                      tmp_folder=None, target="local", max_jobs=mp.cpu_count(),
                      block_shape=None, unit="micrometer",
                      source_name=None, file_format="bdv.n5",
                      signed_to_unsigned=False
                      ):
    """ Import image data to mobie format.

    Arguments:
        in_path [str] - input data to be added.
        in_key [str] - key of the input data to be added.
        out_path [str] - where to add the data.
        resolution [list[float]] - resolution in micrometer
        scale_factors [list[list[int]]] - scale factors used for down-sampling the data
        chunks [tuple[int]] - chunks of the data to be added
        tmp_folder [str] - folder for temporary files (default: None)
        target [str] - computation target (default: "local")
        max_jobs [int] - number of jobs (default: number of cores)
        block_shape [tuple[int]] - block shape used for computation.
            By default, same as chunks. (default:None)
        unit [str] - physical unit of the coordinate system (default: micrometer)
        source_name [str] - name of the source (default: None)
        file_format [str] - the file format (default: "bdv.n5")
    """
    # we allow 2d data for ome.zarr file format
    if file_format != "ome.zarr":
        in_path, in_key = ensure_volume(in_path, in_key, tmp_folder, chunks)
    downscale(in_path, in_key, out_path,
              resolution, scale_factors, chunks,
              tmp_folder, target, max_jobs, block_shape,
              library="skimage", unit=unit, source_name=source_name,
              metadata_format=file_format, signed_to_unsigned=signed_to_unsigned)
