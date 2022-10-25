[![Build Status](https://github.com/mobie/mobie-utils-python/workflows/build_and_test/badge.svg)](https://github.com/mobie/mobie-utils-python/actions)
[![Anaconda-Server Badge](https://anaconda.org/conda-forge/mobie_utils/badges/version.svg)](https://anaconda.org/conda-forge/mobie_utils)

# mobie-utils-python

A python library to generate projects for [MoBIE](https://github.com/mobie-org/mobie).


## Installation

**From conda:**

```
$ conda install -c conda-forge mobie_utils
```

**For development:**

You can set up (and activate) a conda environment with all dependencies from `environment.yaml`:
```sh
$ conda env create -f environment.yaml
$ conda activate mobie
```
and  install the library via
```sh
$ pip install -e .
```

## Usage

The library contains functionality to generate MoBIE projects, add data to it and create complex views.
For complete examples, please check out the [examples](https://github.com/mobie/mobie-utils-python/blob/master/examples):
- [normal project creation](https://github.com/mobie/mobie-utils-python/blob/master/examples/create_mobie_project.ipynb): create a MoBIE project for multi-modal data from a CLEM experiment
- [htm project creation](https://github.com/mobie/mobie-utils-python/blob/master/examples/create_mobie_htm_project.ipynb): create a MoBIE project for high-throughput microscopy from a imaging based SARS-CoV-2 antibody assay.
- [spatial transcriptomics project creation](https://github.com/mobie/mobie-utils-python/blob/master/examples/create_spatial_transcriptomics_project.ipynb): create a MoBIE project for spatial transcriptomics data.

Below is a short code snippet that shows how to use it in a python script.

```python
import mobie

# root folder for the mobie project
mobie_root = "/path/to/project-datasets/data"
# name of the dataset to be added
dataset_name = "my_dataset"

# file path and key for the input data
# key can be an internal path for hdf5 or zarr/n5 containers
# or a file pattern for image stacks
data_path = "/path/to/input_data.h5"
data_key = "/internal/h5/path"

# resolution of this initial data (in micrometer), chunks size and factors for downscaling
resolution = (.5, .25, .25)
chunks = (64, 128, 128)
scale_factors = [[1, 2, 2], [2, 2, 2], [2, 2, 2]]

mobie.add_image(data_path, data_key,
                mobie_root, dataset_name, image_name,
                resolution, chunks, scale_factors)

```

### From the command line

The package also installs some command line scripts that can create MoBIE projects, add data to it and more:
- `mobie.add_image` add image data to MoBIE dataset. Initialize the dataset if it does not exist yet.
- `mobie.add_registered_source` apply registration in elastix format and add the resulting data to MoBIE dataset.
- `mobie.add_segmentation` add segmentation image data to MoBIE dataset.
- `mobie.add_traces` add data containing traces (= skeletonized objects).
- `mobie.validate_project` validate that a MoBIE project follows the spec
- `mobie.validate_dataset` validate that a MoBIE dataset follows the spec
- `mobie.add_remote_metadata` adds the required metadata for accessing the data via s3.

Run `<COMMAND-NAME> --help` to get more information on how to use them.

## Updating MoBIE projects

This library also provides functionality to update MoBIE projects to new specification versions. Updating is performed with the command line function `mobie.migrate_project`. Its only mandatory argument is the filepat to the project data:
```
mobie.migrate_project data
```
**Warning**: this command will update the metadata files without creating backups. You should either run this on projects within git, or create a manual backup of your project.

The most recent update is from version `0.2.1` to `0.3.0`. Previous version updates can be performed by passing the `-v` flag to the `migrate_project` command.

As most likely the specs for the project metadata files change during an update, you need to purge the cached specs files by deleting them in the `.mobie` folder in your `$HOME` directory. 

## Citation

If you use the MoBIE framework in your research, please cite [the MoBIE bioRxiv preprint](https://www.biorxiv.org/content/10.1101/2022.05.27.493763v1).
