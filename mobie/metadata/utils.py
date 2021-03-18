import json
import os
import numpy as np


def write_metadata(path, metadata):
    with open(path, 'w') as f:
        json.dump(metadata, f,
                  indent=2, sort_keys=True,
                  cls=NPTypesEncoder)


def read_metadata(path):
    if os.path.exists(path):
        with open(path) as f:
            metadata = json.load(f)
    else:
        metadata = {}
    return metadata


# enable dumping np dtypes
class NPTypesEncoder(json.JSONEncoder):
    int_types = (np.int8, np.int16, np.int32, np.int64,
                 np.uint8, np.uint16, np.uint32, np.uint64)
    float_types = (np.float32, np.float64)

    def default(self, obj):
        if isinstance(obj, self.int_types):
            return int(obj)
        if isinstance(obj, self.float_types):
            return float(obj)
        return json.JSONEncoder.default(self, obj)
