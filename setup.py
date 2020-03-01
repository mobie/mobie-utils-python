import runpy
from setuptools import setup, find_packages

version = runpy.run_path("mmb/__version__.py")["__version__"]

# minimal setup script for the mmpb package
setup(
    name="mmb",
    packages=find_packages(exclude=["test"]),
    version=version,
    author="Constantin Pape",
    url="https://github.com/platybrowser/mmb-python"
)
