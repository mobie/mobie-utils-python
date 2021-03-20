

def to_affine_source_transform(name, sources, parameters, timepoints=None):
    """
    """
    assert len(parameters) == 12
    assert all(isinstance(param, float) for param in parameters)
    trafo = {
        "affine": {
            "name": name,
            "parameters": parameters,
            "sources": sources
        }
    }
    if timepoints is not None:
        assert isinstance(timepoints, (list, tuple))
        trafo["affine"]["timepoints"] = timepoints
    return trafo


# TODO needs to be refactored a bit to support more complex views without too much code duplication
# TODO support more kwargs and explain in more details
def get_default_view(source_type, source_name, **kwargs):
    """ Create default view metadata for a single source.

    Arguments:
        source_type [str] - type of the source, either 'image' or 'segmentation'
        source_name [str] - name of the source.
        **kwargs - additional keyword arguments for that view
    """
    view = {}
    if source_type == 'image':
        color = kwargs.pop("color", "white")
        contrast_limits = kwargs.pop("contrastLimits",  [0.0, 255.0])
        source_display = {
            "imageDisplays": {
                "color": color,
                "contrastLimits": contrast_limits,
                "name": source_name,
                "sources": [source_name]
            }
        }
    elif source_type == 'segmentation':
        # TODO find a good default alpha value
        alpha = kwargs.pop("alpha", 0.75)
        color = kwargs.pop("color", "glasbey")
        source_display = {
            "segmentationDisplays": {
                "alpha": alpha,
                "color": color,
                "name": source_name,
                "sources": [source_name]
            }
        }
        additional_seg_kwargs = ["colorByColumn", "selectedSegmentIds", "showSelectedSegmentsIn3d", "tables"]
        for kwarg_name in additional_seg_kwargs:
            kwarg_val = kwargs.pop(kwarg_name, None)
            if kwarg_val is not None:
                source_display["segmentationDisplays"][kwarg_name] = kwarg_val
    else:
        raise ValueError(f"Expect source_type to be 'image' or 'segmentation', got {source_type}")
    view["sourceDisplays"] = [source_display]

    source_transforms = kwargs.pop("sourceTransforms", None)
    if source_transforms is not None:
        source_transform_list = []
        for i, transform in enumerate(source_transforms):
            if "name" not in transform:
                transform["name"] = f'affine-{i}'
            transform["sources"] = [source_name]
            source_transform_list.append(to_affine_source_transform(**transform))
        view["sourceTransforms"] = source_transform_list

    viewer_transform = kwargs.pop("viewerTransform", None)
    if viewer_transform is not None:
        view["viewerTransform"] = viewer_transform

    if kwargs:
        raise ValueError(f"Invalid keyword arguments: {list(kwargs.keys())}")

    return view
