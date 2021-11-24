#! /bin/python

import os
import sys
import json

import luigi
import numpy as np
import pandas as pd
from skimage.measure import regionprops

import cluster_tools.utils.function_utils as fu
import cluster_tools.utils.volume_utils as vu
from cluster_tools.cluster_tasks import SlurmTask, LocalTask, LSFTask
from cluster_tools.utils.task_utils import DummyTask


#
# table implementation
#

class TableImplBase(luigi.Task):
    """ table_impl base class
    """

    task_name = "table_impl"
    src_file = os.path.abspath(__file__)

    input_files = luigi.ListParameter()
    output_files = luigi.ListParameter()
    input_key = luigi.Parameter()
    resolution = luigi.ListParameter()
    dependency = luigi.TaskParameter(default=DummyTask())

    def requires(self):
        return self.dependency

    def require_output_folders(self):
        output_folders = [os.path.split(out_file)[0] for out_file in self.output_files]
        output_folders = list(set(output_folders))
        for out_folder in output_folders:
            os.makedirs(out_folder, exist_ok=True)

    def run_impl(self):
        # get the global config and init configs
        shebang = self.global_config_values()[0]
        self.init(shebang)

        self.require_output_folders()

        # luigi may randomly shuffles the file lists, so we need to make sure they are ordered here
        input_files = list(self.input_files)
        input_files.sort()
        output_files = list(self.output_files)
        output_files.sort()

        # load and update the task config
        task_config = self.get_task_config()
        task_config.update({"input_files": input_files,
                            "output_files": output_files,
                            "resolution": self.resolution,
                            "input_key": self.input_key})

        block_list = list(range(len(input_files)))
        self._write_log("scheduled %i blocks to run" % len(block_list))

        # prime and run the jobs
        n_jobs = min(len(block_list), self.max_jobs)
        self.prepare_jobs(n_jobs, block_list, task_config)
        self.submit_jobs(n_jobs)

        # wait till jobs finish and check for job success
        self.wait_for_jobs()
        self.check_jobs(n_jobs)


class TableImplLocal(TableImplBase, LocalTask):
    """
    copy_volume local machine
    """
    pass


class TableImplSlurm(TableImplBase, SlurmTask):
    """
    copy on slurm cluster
    """
    pass


class TableImplLSF(TableImplBase, LSFTask):
    """
    copy_volume on lsf cluster
    """
    pass


def get_table_impl_task(target):
    if target == "local":
        return TableImplLocal
    elif target == "slurm":
        return TableImplSlurm
    elif target == "lsf":
        return TableImplLSF
    else:
        raise ValueError


#
# Implementation
#


def load_seg(input_file, key):
    with vu.file_reader(input_file, "r") as f:
        return f[key][:]


def compute_table(input_file, table_path, key, resolution):
    seg = load_seg(input_file, key)
    ndim = seg.ndim

    props = regionprops(seg)
    tab = np.array([
        [p.label]
        + [ce / res for ce, res in zip(p.centroid, resolution)]
        + [float(bb) / res for bb, res in zip(p.bbox[:ndim], resolution)]
        + [float(bb) / res for bb, res in zip(p.bbox[ndim:], resolution)]
        + [p.area]
        for p in props
    ])
    col_names = ["label_id",
                 "anchor_z", "anchor_y", "anchor_x",
                 "bb_min_z", "bb_min_y", "bb_min_x",
                 "bb_max_z", "bb_max_y", "bb_max_x",
                 "n_pixels"]
    if ndim == 2:
        col_names = [name for name in col_names if not name.endswith("z")]
    assert tab.shape[1] == len(col_names), f"{tab.shape}, {len(col_names)}"
    tab = pd.DataFrame(tab, columns=col_names)
    tab.to_csv(table_path, sep="\t", index=False, na_rep="nan")


def table_impl(job_id, config_path):
    fu.log("start processing job %i" % job_id)
    fu.log("reading config from %s" % config_path)
    with open(config_path, "r") as f:
        config = json.load(f)

    # read the input cofig
    input_files = config["input_files"]
    output_files = config["output_files"]
    input_key = config["input_key"]
    resolution = config["resolution"]

    # these are the ids of files to copy in this job
    # the field is called block list because we are re-using functionality from 3d blocking logic
    file_ids = config["block_list"]

    for file_id in file_ids:
        compute_table(input_files[file_id], output_files[file_id],
                      input_key, resolution)

    # log success
    fu.log_job_success(job_id)


if __name__ == "__main__":
    path = sys.argv[1]
    assert os.path.exists(path), path
    job_id = int(os.path.split(path)[1].split(".")[0].split("_")[-1])
    table_impl(job_id, path)
