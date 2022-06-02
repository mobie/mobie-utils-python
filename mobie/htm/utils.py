import json
import os
from concurrent import futures

import numpy as np
from elf.io import open_file
from tqdm import tqdm
from ..metadata import read_dataset_metadata


def compute_contrast_limits(
    source_prefix, dataset_folder, lower_percentile, upper_percentile, n_threads, cache_path=None
):
    if cache_path is not None and os.path.exists(cache_path):
        with open(cache_path) as f:
            return json.load(f)
    sources = read_dataset_metadata(dataset_folder)["sources"]

    def compute_clim_im(source_name):
        path = os.path.join(
            dataset_folder,
            sources[source_name]["image"]["imageData"]["ome.zarr"]["relativePath"]
        )
        with open_file(path, "r") as f:
            data = f["s0"][:]
            cmin = np.percentile(data, lower_percentile)
            cmax = np.percentile(data, upper_percentile)
        return cmin, cmax

    source_names = [name for name in sources.keys() if name.startswith(source_prefix)]
    with futures.ThreadPoolExecutor(n_threads) as tp:
        results = list(tqdm(
            tp.map(compute_clim_im, source_names),
            total=len(source_names),
            desc=f"Compute contrast limits for {source_prefix}"
        ))

    cmin = np.median([res[0] for res in results])
    cmax = np.median([res[1] for res in results])
    clim = [float(cmin), float(cmax)]

    if cache_path is not None:
        with open(cache_path, "w") as f:
            json.dump(clim, f)

    return clim
