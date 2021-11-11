import os
try:
    import boto3
    from botocore import UNSIGNED
    from botocore.client import Config
except ImportError:
    boto3 = None

CACHE_DIR = os.path.expanduser('~/.mobie/downloads')


def have_boto():
    return boto3 is not None


def get_client(endpoint, anon):
    if anon:
        client = boto3.client(service_name='s3',
                              endpoint_url=endpoint,
                              config=Config(signature_version=UNSIGNED))
    else:
        client = boto3.client(service_name='s3',
                              endpoint_url=endpoint)
    return client


def download_file(client, bucket, object_name, force=False):
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
