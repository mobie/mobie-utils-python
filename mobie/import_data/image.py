import multiprocessing as mp
from elf.io import open_file
from .utils import downscale, ensure_volume


def import_image_data(in_path, in_key, out_path,
                      resolution, scale_factors, chunks,
                      tmp_folder=None, target="local", max_jobs=mp.cpu_count(),
                      block_shape=None, unit="micrometer",
                      source_name=None, file_format="ome.zarr",
                      int_to_uint=False, channel=None,
                      selected_input_channel=None,
                      roi_begin=None, roi_end=None):
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
        file_format [str] - the file format (default: "ome.zarr")
        int_to_uint [bool] - whether to convert signed to unsigned integer (default: False)
        channel [int] - the channel to load from the data.
            Currently only supported for the ome.zarr format (default: None)
        selected_input_channel [list[int]] - A single channel (idx) to be added. If channel is not axis 0: [idx, dim]
        roi_begin [list[int]] - Start of ROI to be extracted
        roi_end [list[int]] - End of ROI to be extracted
    """

    # we allow 2d data for ome.zarr file format
    if file_format != "ome.zarr":
        in_path, in_key = ensure_volume(in_path, in_key, tmp_folder, chunks)
        if not all((selected_input_channel is None, roi_begin is None, roi_end is None)):
            raise NotImplementedError("Selection of sub-arrays only possible with OME-Zarr output.")

    if selected_input_channel:
        if type(selected_input_channel) is int:
            selected_input_channel = [0, selected_input_channel]
        elif len(selected_input_channel) < 2:
            # if only one element, we assume relevant image stack dimension is 0 (like channel for multi-channel tifs).
            selected_input_channel = [0, selected_input_channel[0]]
        elif len(selected_input_channel) > 2:
            raise ValueError("Only single channel selection possible.")

        with open_file(in_path, mode="r") as f:
            shape = f[in_key].shape

        roi_begin = [0] * len(shape)
        roi_end = list(shape)

        if selected_input_channel[0] > len(shape) - 1:
            raise ValueError("Wrong channel dimension.")

        if selected_input_channel[1] > shape[selected_input_channel[0]] - 1:
            raise ValueError("Channel index exceeds axis length.")

        roi_begin[selected_input_channel[0]] = selected_input_channel[1]
        roi_end[selected_input_channel[0]] = selected_input_channel[1] + 1


    downscale(in_path, in_key, out_path,
              resolution, scale_factors, chunks,
              tmp_folder, target, max_jobs, block_shape,
              library="skimage", unit=unit, source_name=source_name,
              metadata_format=file_format,
              roi_begin=roi_begin, roi_end=roi_end,
              int_to_uint=int_to_uint,
              channel=channel)
