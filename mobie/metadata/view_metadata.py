

def get_default_view(source_type, source_name):
    if source_type == 'image':
        view = {
            "displayGroups": [
                {
                    "imageDisplayGroup": {
                        "color": "white",
                        "contrastLimits": [0.0, 255.0],
                        "name": source_name,
                        "sources": [source_name],
                        "timepoint": 0
                    }
                }
            ]
        }
    elif source_type == 'segmentation':
        view = {
            "displayGroups": [
                {
                    "segmentationDisplayGroup": {
                        "alpha": 1000.,  # TODO what is our alpha range and what's the default value
                        "color": "glasbey",
                        "sources": [source_name],
                        "timepoint": 0
                    }
                }
            ]
        }
    else:
        raise ValueError(f"Expect source_type to be 'image' or 'segmentation', got {source_type}")
    return view
