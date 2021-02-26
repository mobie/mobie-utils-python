# mobie-utils-python

A python library to generate projects for [MoBIE](https://github.com/mobie-org/mobie).


## Installation

**From conda:**

```
conda install -c conda-forge -c cpape mobie_utils
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

The library contains functionality to generate MoBIE projects and add data to it.
You can find a complete example notebook that explains how to generate a project [here](https://github.com/mobie/mobie-utils-python/blob/master/examples/create_mobie_project.ipynb).
Below is a short code snippet that shows how to use it in a python script.

```python
import mobie

# root folder for the mobie project
mobie_root = '/path/to/project-datasets/data'
# name of the dataset to be added
dataset_name = 'my_dataset'
# internal name for the initial data for this dataset
data_name = 'raw-data'

# file path and key for the input data
# key can be an internal path for hdf5 or zarr/n5 containers
# or a file pattern for image stacks
data_path = '/path/to/input_data.h5'
data_key = '/internal/h5/path'

# resolution of this initial data (in micrometer), chunks size and factors for down-scaling
resolution = (.5, .25, .25)
chunks = (64, 128, 128)
scale_factors = [[1, 2, 2], [2, 2, 2], [2, 2, 2]]

# initialize a dataset for this project with raw data
mobie.initialize_dataset(data_path, data_key,
                         mobie_root, dataset_name, data_name,
                         resolution, chunks, scale_factors)

# add a different image with different base resolution to the dataset
image_path = '/path/to/image.h5'
image_key = 'h5/path'

image_name = 'image-data'
resolution = (.5, .5, .5)
chunks = 3 * (64,)
scale_factors = 3 * [[2, 2, 2]]

mobie.add_image(image_path, image_key,
                mobie_root, dataset_name, image_name,
                resolution, chunks, scale_factors)

```

### From the command line

The package also installs some command line tools to create mobie projects and add data to a project:
- `mobie.add_image` add image data to MoBIE dataset. Initialize the dataset if it does not exist yet.
- `mobie.add_mask` add binary mask image data to MoBIE dataset. 
- `mobie.add_registered_volume` apply registration in elastix format and add the resulting data to MoBIE dataset.
- `mobie.add_segmentation` add segmentation image data to MoBIE dataset.
- `mobie.add_traces` add data containing traces (= skeletonized objects).

Run `<COMMAND-NAME> --help` to get more information on how to use them.


## Citation

If you use the MoBIE framework in your research, please cite [Whole-body integration of gene expression and single-cell morphology](https://www.biorxiv.org/content/10.1101/2020.02.26.961037v1).
