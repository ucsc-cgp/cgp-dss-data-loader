"""
Utility functions for uploading a file to a staging bucket in preparation
for loading into the HCA DSS.

Leveraged from GitHub Human Cell Atlas dcp-cli/hca/dss/upload_to_cloud.py
"""
import logging
import mimetypes
import os
import uuid

import boto3
from boto3.s3.transfer import TransferConfig

from dcplib.checksumming_io import ChecksummingBufferedReader, S3Etag


def encode_tags(tags):
    return [dict(Key=k, Value=v) for k, v in tags.items()]


def _mime_type(filename):
    type_, encoding = mimetypes.guess_type(filename)
    if encoding:
        return encoding
    if type_:
        return type_
    return "application/octet-stream"


def _copy_from_s3(path, s3, tx_cfg):
    bucket_end = path.find("/", 5)
    bucket_name = path[5: bucket_end]
    dir_path = path[bucket_end + 1:]

    src_bucket = s3.Bucket(bucket_name)
    file_uuids = []
    key_names = []
    logging.info("Key Names:")
    for obj in src_bucket.objects.filter(Prefix=dir_path):
        # Empty files with no name were throwing errors
        if obj.key == dir_path:
            continue

        logging.info(obj.key)

        file_uuids.append(str(uuid.uuid4()))
        key_names.append(obj.key)

    return file_uuids, key_names


def upload_to_cloud(file_handle, file_uuid, staging_bucket, content_type=None):
    """
    Upload a file to the staging bucket, computing the DSS-required checksums
    in the process, then tag the file in the staging bucket with the checksums.
    This is in preparation from subsequently uploading the file from the staging
    bucket into the DSS.

    :param file_handle: File handle for file to be uploaded.
    :param file_uuid: An RFC4122-compliant UUID to be used to identify the file
    :param staging_bucket: The aws bucket to upload the files to.
    :param content_type: The type of content in the file, in the format returned by _mime_type()
    :return: a tuple of the file_uuid and key
    """
    tx_cfg = TransferConfig(multipart_threshold=S3Etag.etag_stride,
                            multipart_chunksize=S3Etag.etag_stride)
    s3 = boto3.resource("s3")

    destination_bucket = s3.Bucket(staging_bucket)
    with ChecksummingBufferedReader(file_handle) as fh:
        key_name = "{}/{}".format(file_uuid, os.path.basename(fh.raw.name))
        destination_bucket.upload_fileobj(
            fh,
            key_name,
            Config=tx_cfg,
            ExtraArgs={
                'ContentType': content_type if content_type is not None else _mime_type(fh.raw.name)
            }
        )
        sums = fh.get_checksums()
        metadata = {
            "hca-dss-s3_etag": sums["s3_etag"],
            "hca-dss-sha1": sums["sha1"],
            "hca-dss-sha256": sums["sha256"],
            "hca-dss-crc32c": sums["crc32c"],
        }

        s3.meta.client.put_object_tagging(Bucket=destination_bucket.name,
                                          Key=key_name,
                                          Tagging=dict(TagSet=encode_tags(metadata))
                                          )

    return file_uuid, key_name
