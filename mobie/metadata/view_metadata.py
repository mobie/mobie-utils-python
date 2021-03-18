

def get_default_view(source_type, source_name):
    if source_type == 'image':
        view = {
            "sourceDisplays": [
                {
                    "imageDisplays": {
                        "color": "white",
                        "contrastLimits": [0.0, 255.0],
                        "name": source_name,
                        "sources": [source_name]
                    }
                }
            ]
        }
    elif source_type == 'segmentation':
        view = {
            "sourceDisplays": [
                {
                    "segmentationDisplays": {
                        "alpha": 0.75,  # TODO find a good default alpha value
                        "color": "glasbey",
                        "sources": [source_name]
                    }
                }
            ]
        }
    else:
        raise ValueError(f"Expect source_type to be 'image' or 'segmentation', got {source_type}")
    return view
