"""mobie is a python library for generating, populating, validating and migrating
[MoBIE](https://github.com/mobie/mobie) projects: it converts raw image, segmentation
and spot data into MoBIE-compatible storage and writes the spec-compliant metadata
that the MoBIE Fiji viewer reads.
"""

from .image_data import add_image, add_bdv_image
from .open_organelle import add_open_organelle_data
from .registration import add_registered_source
from .segmentation import add_segmentation
from .spots import add_spots
from .source_utils import remove_source, rename_source
from .traces import add_traces
from .view_utils import create_view, create_grid_view, combine_views, merge_view_file

from .__version__ import __version__, SPEC_VERSION
