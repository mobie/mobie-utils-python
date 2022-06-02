from mobie.metadata.slice_grid_view import create_slice_grid


def add_slice_grid_view():
    dataset_folder = "/home/pape/Work/data/mobie/mobie_example_project/data/example-dataset"
    source = "em-27_tomogram"
    create_slice_grid(
        dataset_folder, source, n_slices=9,
        view_name="slice-grid", menu_name="slice-grids",
        overwrite=True
    )


if __name__ == "__main__":
    add_slice_grid_view()
