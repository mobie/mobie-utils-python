from .util import add_max_id, downscale, ensure_volume


def import_segmentation(in_path, in_key, out_path,
                        resolution, scale_factors, chunks,
                        tmp_folder, target, max_jobs,
                        block_shape=None, with_max_id=True):
    """ Import segmentation data into mobie format.

    Arguments:
        in_path [str] - input segmentation to be added.
        in_key [str] - key of the segmentation to be added.
        out_path [str] - where to add the segmentation.
        resolution [list[float]] - resolution in micrometer
        scale_factors [list[list[int]]] - scale factors used for down-sampling the data
        chunks [tuple[int]] - chunks of the data to be added
        tmp_folder [str] - folder for temporary files
        target [str] - computation target
        max_jobs [int] - number of jobs
        block_shape [tuple[int]] - block shape used for computation.
            By default, same as chunks. (default:None)
        with_max_id [bool] - whether to add the max id attribute
    """
    in_path, in_key = ensure_volume(in_path, in_key,
                                    tmp_folder, chunks)

    downscale(in_path, in_key, out_path,
              resolution, scale_factors, chunks,
              tmp_folder, target, max_jobs, block_shape,
              library='vigra', library_kwargs={'order': 0})

    if with_max_id:
        add_max_id(in_path, in_key, out_path, 'setup0/timepoint0/s0',
                   tmp_folder, target, max_jobs)
