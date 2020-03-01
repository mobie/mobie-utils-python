# valid field names

# common valid field names for 'images.json' entry and 'bookmarks.json' entry
COMMON_KEYS = {'Color', 'ColorMap',
               'ColorMapMinValue', 'ColorMapMaxValue',
               'MinValue', 'MaxValue'}
# valid field names in 'images.json' entry
IMAGE_DICT_KEYS = COMMON_KEYS.union({'Storage', 'TableFolder', 'Type'})
# valid field names in 'bookmarks.json' entry
BOOKMARK_DICT_KEY = COMMON_KEYS.union({'ColorByColumn',
                                       'SelectedLabelIds',
                                       'ShowImageIn3d',
                                       'ShowSelectedSegmentsIn3d',
                                       'Tables'})

# valid values for fields

# TODO extend this
# color names supported by platy browser
COLORS = {'Blue',
          'Green',
          'Magenta',
          'RandomFromGlasbey',
          'Red',
          'White',
          'Yellow'}

# color map names supported by platy browser
COLORMAPS = {'BlueWhiteRed', 'BlueWhiteRedZeroTransparent',
             'Glasbey', 'GlasbeyZeroTransparent',
             'Viridis', 'ViridisZeroTransparent'}

# image types supported by platybrowser
TYPES = {'Image', 'Mask', 'Segmentation'}
