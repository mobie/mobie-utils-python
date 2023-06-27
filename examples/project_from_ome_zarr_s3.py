import argparse
import os
import mobie.metadata as metadata


def project_from_ome_zarr(url, dataset_name, root):
    # check if the project exists already, create if not
    if not metadata.project_exists(root):
        metadata.create_project_metadata(root)

    # check if the dataset exists already, create if not
    ds_folder = os.path.join(root, dataset_name)
    if not metadata.dataset_exists(root, dataset_name):
        metadata.create_dataset_structure(root, dataset_name)
        metadata.create_dataset_metadata(ds_folder)
        metadata.add_dataset(root, dataset_name, is_default=False)

    # NOTE: would need to adapt this to instead add a segmentation
    # add the new image to the dataset
    source_name = os.path.splitext(os.path.split(url)[1])[0]
    metadata.add_source_to_dataset(
        ds_folder, "image", source_name, url, file_format="ome.zarr.s3"
    )

    # add a default view if we don't have it yet
    ds_metadata = metadata.read_dataset_metadata(ds_folder)
    if "default" not in ds_metadata["views"]:
        default_view = ds_metadata["views"][source_name]
        ds_metadata["views"]["default"] = default_view
        metadata.write_dataset_metadata(ds_folder, ds_metadata)


# Example how to call the command for adding an image from bioimage archive:
# python project_from_ome_zarr_s3.py https://uk1s3.embassy.ebi.ac.uk/bia-zarr-test/S-BSST410/IM1.zarr -d my-dataset
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("url")
    parser.add_argument("-d", "--dataset_name", required=True)
    parser.add_argument("-o", "--output_root", default="./data")
    args = parser.parse_args()
    project_from_ome_zarr(args.url, args.dataset_name, args.output_root)


if __name__ == "__main__":
    main()
