import os
from elf.io import open_file, is_z5py, is_group
from .util import downscale, add_max_id

try:
    from paintera_tools import serialize_from_commit, postprocess
except ImportError:
    serialize_from_commit = None


def is_paintera(path, key):
    expected_keys = {'data',
                     'fragment-segment-assignment',
                     'unique-labels',
                     'label-to-block-mapping'}
    with open_file(path, 'r') as f:
        if not is_z5py(f):
            return False
        g = f[key]
        if not is_group(g):
            return False
        keys = set(g.keys())
        if len(expected_keys - keys) > 0:
            return False
    return True


def import_segmentation_from_paintera(in_path, in_key, out_path,
                                      resolution, scale_factors, chunks,
                                      tmp_folder, target, max_jobs,
                                      block_shape=None, postprocess_config=None,
                                      map_to_background=None, unit='micrometer'):
    """ Import segmentation data into mobie format from a paintera dataset

    Arguments:
        in_path [str] - input paintera dataset to be added.
        in_key [str] - key of the paintera dataset to be added.
        out_path [str] - where to add the segmentation.
        resolution [list[float]] - resolution in micrometer
        scale_factors [list[list[int]]] - scale factors used for down-sampling the data
        chunks [tuple[int]] - chunks of the data to be added
        tmp_folder [str] - folder for temporary files
        target [str] - computation target
        max_jobs [int] - number of jobs
        block_shape [tuple[int]] - block shape used for computation.
            By default, same as chunks. (default:None)
        postprocess_config: config for segmentation post-processing (default: None)
        map_to_background: additional ids to be mapped to background label (default: None)
        unit [str] - physical unit of the coordinate system (default: micrometer)
    """
    if serialize_from_commit is None:
        msg = """Importing a segmentation from paintera is only possible wit paintera_tools:
        https://github.com/constantinpape/paintera_tools
        """
        raise AttributeError(msg)

    out_key = 'setup0/timepoint0/s0'
    # run post-processing if specified for this segmentation name
    if postprocess_config is not None:
        boundary_path = postprocess_config['BoundaryPath']
        boundary_key = postprocess_config['BoundaryKey']

        min_segment_size = postprocess_config.get('MinSegmentSize', None)
        max_segment_number = postprocess_config.get('MaxSegmentNumber', None)

        label_segmentation = postprocess_config['LabelSegmentation']
        tmp_postprocess = os.path.join(tmp_folder, 'postprocess_paintera')

        print("Run postprocessing:")
        if label_segmentation:
            print("with connected components")
        if max_segment_number is not None:
            print("With max segment number:", max_segment_number)
        if min_segment_size is not None:
            print("With min segment size:", min_segment_size)

        postprocess(in_path, in_key,
                    boundary_path, boundary_key,
                    tmp_folder=tmp_postprocess,
                    target=target, max_jobs=max_jobs,
                    n_threads=8, size_threshold=min_segment_size,
                    target_number=max_segment_number,
                    label=label_segmentation,
                    output_path=out_path, output_key=out_key)

    else:
        # export segmentation from in commit for all scales
        serialize_from_commit(in_path, in_key, out_path, out_key, tmp_folder,
                              max_jobs, target, relabel_output=True,
                              map_to_background=map_to_background)

    downscale(out_path, out_key, out_path,
              resolution, scale_factors, chunks,
              tmp_folder, target, max_jobs, block_shape,
              library='vigra', library_kwargs={'order': 0},
              unit=unit)

    add_max_id(in_path, in_key, out_path, out_key,
               tmp_folder, target, max_jobs)
