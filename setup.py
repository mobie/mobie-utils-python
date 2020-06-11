import runpy
from setuptools import setup, find_packages

version = runpy.run_path("mobie/__version__.py")["__version__"]

# minimal setup script for the mmpb package
setup(
    name="mobie",
    packages=find_packages(exclude=["test"]),
    version=version,
    author="Constantin Pape",
    url="https://github.com/platybrowser/mobie-utils-python",
    license='MIT',
    entry_points={
        "console_scripts": [
            "add_image_data = mobie.image_data:main",
            "add_mask = mobie.mask:main",
            "add_segmentation = mobie.segmentation:main",
            "add_traces = mobie.traces:main",
            "migrate_to_mobie = mobie.migration.migrate:main",
            "migrate_dataset_to_mobie = mobie.migration.migrate_dataset:main",
        ]
    },
)
