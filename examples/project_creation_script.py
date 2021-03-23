import argparse
import os
import imageio
import mobie
import mobie.metadata as metadata

parser = argparse.ArgumentParser()
parser.add_argument('-i', '--input', required=True)
parser.add_argument('-o', '--output', required=True)
parser.add_argument('-d', '--dataset_name', default='example-dataset')
parser.add_argument('-t', '--target', default='local')
parser.add_argument('-j', '--max_jobs', default=4, type=int)

args = parser.parse_args()
example_input_data = args.input
mobie_project_folder = args.output
dataset_name = args.dataset_name
dataset_folder = os.path.join(mobie_project_folder, dataset_name)

target = args.target
max_jobs = args.max_jobs

# The 'default' image for our example dataset is a 2d EM slice showing an overview of the dataset.
input_file = os.path.join(example_input_data, 'em_overview.tif')

# This is the name that will be given to the image source in mobie.
raw_name = 'em-raw'
unit = 'nanometer'
resolution = (1., 10., 10.)
chunks = (1, 512, 512)
scale_factors = 4 * [[1, 2, 2]]

mobie.add_image(
    input_path=input_file,
    input_key='',  # the input is a single tif image, so we leave input_key blank
    root=mobie_project_folder,
    dataset_name=dataset_name,
    image_name=raw_name,
    resolution=resolution,
    chunks=chunks,
    scale_factors=scale_factors,
    is_default_dataset=True,  # mark this dataset as the default dataset that will be loaded by mobie
    target=target,
    max_jobs=max_jobs,
    unit=unit
)

tomo_names = ['27_tomogram.tif', '29_tomogram.tif']
unit = 'nanometer'
resolution = [5., 5., 5.]
chunks = (32, 128, 128)
scale_factors = [[1, 2, 2], [1, 2, 2],
                 [1, 2, 2], [1, 2, 2],
                 [2, 2, 2]]

transformations = [
    [5.098000335693359, 0.0, 0.0, 54413.567834472655,
     0.0, 5.098000335693359, 0.0, 51514.319843292236,
     0.0, 0.0, 5.098000335693359, 0.0],
    [5.098000335693359, 0.0, 0.0, 39024.47988128662,
     0.0, 5.098000335693359, 0.0, 44361.50386505127,
     0.0, 0.0, 5.098000335693359, 0.0]
]

# add the two tomograms
for name, trafo in zip(tomo_names, transformations):
    im_name = f"em-{os.path.splitext(name)[0]}"
    im_path = os.path.join(example_input_data, name)

    # Here, we set the default contrast limits that will
    # be applied in mobie to the min / max value of the data.
    im = imageio.volread(im_path)
    min_val, max_val = im.min(), im.max()
    view = metadata.get_default_view("image", im_name,
                                     source_transform={'parameters': trafo},
                                     contrastLimits=[min_val, max_val])

    mobie.add_image(
        input_path=im_path,
        input_key="",
        root=mobie_project_folder,
        dataset_name=dataset_name,
        image_name=im_name,
        resolution=resolution,
        scale_factors=scale_factors,
        transformation=trafo,
        chunks=chunks,
        target=target,
        max_jobs=max_jobs,
        view=view,
        unit=unit
    )


input_path = os.path.join(example_input_data, 'fluorescence_downsampled.tif')
im_name = "lm-fluorescence"

# This is again a 2d image, so we set all values for Z to 1.
unit = 'nanometer'
resolution = [1., 100., 100.]
scale_factors = [[1, 2, 2], [1, 2, 2], [1, 2, 2]]
chunks = (1, 512, 512)

# In addition, we set the default display color to green.
view = metadata.get_default_view("image", im_name, color="green")

mobie.add_image(
    input_path=input_path,
    input_key="",
    root=mobie_project_folder,
    dataset_name=dataset_name,
    image_name=im_name,
    resolution=resolution,
    scale_factors=scale_factors,
    view=view,
    chunks=chunks,
    target=target,
    max_jobs=max_jobs,
    unit=unit
)


# we add a mask that focuses the region of the dataset with proper data
input_path = os.path.join(example_input_data, 'em_mask.tif')
mask_name = "em-mask"

# again, the mask is 2d
unit = "nanometer"
chunks = [1, 256, 256]
resolution = [1., 160., 160.]
scale_factors = [[1, 2, 2]]

mobie.add_image(
    input_path=input_path,
    input_key="",
    root=mobie_project_folder,
    dataset_name=dataset_name,
    image_name=mask_name,
    resolution=resolution,
    chunks=chunks,
    scale_factors=scale_factors,
    unit=unit
)

# we add a segmentation for several objects visible in the em-overview image
input_path = os.path.join(example_input_data, 'em_segmentation.tif')
segmentation_name = "em-segmentation"

unit = "nanometer"
resolution = [1., 30., 30.]
chunks = [1, 256, 256]
scale_factors = [[1, 2, 2], [1, 2, 2], [1, 2, 2], [1, 2, 2]]

mobie.add_segmentation(
    input_path=input_path,
    input_key="",
    root=mobie_project_folder,
    dataset_name=dataset_name,
    segmentation_name=segmentation_name,
    resolution=resolution,
    chunks=chunks,
    scale_factors=scale_factors,
    add_default_table=True
)


# finally, we update the default bookmark so that both the raw data and the segmentation is
# loaded upon opening the dataset
source_list = [[raw_name], [segmentation_name]]
settings = [
    {"color": "white", "contrastLimits": [0., 255.]},
    {"color": "glasbey", "alpha": 0.75}
]
mobie.metadata.add_dataset_bookmark(dataset_folder, "default",
                                    sources=source_list, display_settings=settings,
                                    overwrite=True)


# create metadata for remote data
bucket_name = 'my-test-bucket'
service_endpoint = 'https://s3.embl.de'
metadata.add_remote_project_metadata(
    mobie_project_folder,
    bucket_name,
    service_endpoint,
)
