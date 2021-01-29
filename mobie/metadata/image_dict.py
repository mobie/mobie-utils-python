import json
import os


def default_image_layer_settings():
    settings = {
         "color": "white",
         "contrastLimits": [0., 255.],
         "type": "image"
     }
    return settings


def default_segmentation_layer_settings():
    settings = {
         "color": "randomFromGlasbey",
         "contrastLimits": [0., 1000.],
         "type": "segmentation"
     }
    return settings


def default_mask_layer_settings():
    settings = {
         "color": "white",
         "contrastLimits": [0., 1.],
         "type": "mask"
     }
    return settings


def default_layer_setting(layer_type):
    if layer_type == 'image':
        return default_image_layer_settings()
    elif layer_type == 'segmentation':
        return default_segmentation_layer_settings()
    elif layer_type == 'mask':
        return default_mask_layer_settings()
    raise ValueError(f"Invalid layer type: {layer_type}")


# TODO
def validate_layer_settings(settings, layer_type):
    pass


def add_to_image_dict(dataset_folder, layer_type, xml_path,
                      settings=None, table_folder=None, overwrite=False,
                      add_remote=False):
    """ Add entry to the image dict.

    Arguments:
        dataset_folder [str] - path to the dataset folder
        layer_type [str] - type of the layer, 'image' or 'segmentation'
        xml_path [str] - path to the xml for the raw data of this dataset.
        settings [dict] - settings for the layer. (default: None)
        table_folder [str] - table folder for segmentations. (default: None)
        overwrite [bool] - whether to overwrite existing entries (default: False)
        add_remote [bool] - whether to add the remote storage entry (default: False)
    """
    if not os.path.exists(xml_path):
        raise ValueError(f"{xml_path} does not exist")
    layer_name = os.path.splitext(os.path.split(xml_path)[1])[0]

    image_folder = os.path.join(dataset_folder, 'images')
    image_dict_path = os.path.join(image_folder, 'images.json')

    if os.path.exists(image_dict_path):
        with open(image_dict_path) as f:
            image_dict = json.load(f)
    else:
        image_dict = {}

    if layer_name in image_dict and not overwrite:
        raise ValueError(f"{layer_name} is already in the image_dict")

    if settings is None:
        settings = default_layer_setting(layer_type)
    else:
        validate_layer_settings(settings, layer_type)
        settings.update({'type': layer_type})

    rel_path = os.path.relpath(xml_path, image_folder)
    storage = {"local": rel_path}
    if add_remote:
        storage["remote"] = rel_path.replace("local", "remote")
    settings.update({
        "storage": storage
    })

    if table_folder is not None:

        if layer_type != 'segmentation':
            msg = f"Table folder is only supported for segmentation layers, got {layer_type}"
            raise ValueError(msg)
        rel_table_folder = os.path.relpath(table_folder, dataset_folder)
        settings.update({
            'tableFolder': rel_table_folder
        })

    image_dict[layer_name] = settings
    with open(image_dict_path, 'w') as f:
        json.dump(image_dict, f, indent=2, sort_keys=True)
