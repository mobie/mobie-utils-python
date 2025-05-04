"""Utility functions for S3.
"""

import os
try:
    import boto3
    from botocore import UNSIGNED
    from botocore.client import Config
except ImportError:
    boto3 = None

CACHE_DIR = os.path.expanduser("~/.mobie/downloads")
"""The cache directory for downloading data.
"""


def have_boto() -> bool:
    """Whether boto3 is installed.

    Returns:
        True if boto3 is returned, False otherwise.
    """
    return boto3 is not None


def get_client(endpoint: str, anon: bool):
    """Get the boto3 client.

    Args:
        endpoint: The endpoint of the s3 bucket.
        anon: Whether to open the bucket in anon mode.

    Returns:
        The boto3 client.
    """
    if anon:
        client = boto3.client(service_name="s3",
                              endpoint_url=endpoint,
                              config=Config(signature_version=UNSIGNED))
    else:
        client = boto3.client(service_name="s3",
                              endpoint_url=endpoint)
    return client


def download_file(client, bucket: str, object_name: str, force: bool = False) -> str:
    """Download a file from an S3 bucket.

    Args:
        client: The boto S3 client.
        bucket: The bucket name.
        object_name: The name of the object to download.
        force: Whether to redownload the object if it is already stored in the cache directory.

    Returns:
        The path to the downloaded file.
    """
    file_name = os.path.join(CACHE_DIR, bucket, object_name)
    if os.path.exists(file_name) and force:
        os.remove(file_name)
    elif os.path.exists(file_name):
        return file_name

    os.makedirs(os.path.split(file_name)[0], exist_ok=True)
    # this raises botocore.exceptions.ClientError if the download fails
    # maybe catch this error and raise something else or just return None and handle later
    client.download_file(bucket, object_name, file_name)

    return file_name
