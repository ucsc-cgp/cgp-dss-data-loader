"""
Classes to load data files and bundles into the HCA Data Storage System (DSS).
The data files may be located in AWS and/or GCP buckets, which may require
authentication and may be requester pays buckets.

These classes support a means of "loading files by reference" even though the current
HCA DSS does not. For more information see:
"Commons HCA DSS Data Loading by Reference"
https://docs.google.com/document/d/1QSa7Ubw-muyD_u0X_dq9WeKyK_dCJXi4Ex7S_pil1uk/edit#heading=h.exnqjy2n2q78

Note: The TOPMed Google controlled access buckets are based on ACLs for user accounts
Before running this loader, configure use of Google user account, run: gcloud auth login
"""

import base64
import binascii
import json
import logging
import os
import time
import typing
import uuid
from io import open
from tempfile import mkdtemp
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import boto3
import botocore
import requests
from boto3.s3.transfer import TransferConfig
from cloud_blobstore import BlobStore, s3
from dcplib.checksumming_io import ChecksummingSink, S3Etag
from google.cloud.storage import Client
from hca.dss import DSSClient
from hca.util import SwaggerAPIException

from .upload_to_cloud import upload_to_cloud, encode_tags

logger = logging.getLogger(__name__)

CREATOR_ID = 20


class DssUploader:
    def __init__(self, dss_endpoint: str, staging_bucket: str, google_project_id: str, dry_run: bool) -> None:
        """
        Functions for uploading files to a given DSS.

        :param dss_endpoint: The URL to a Swagger DSS API.  e.g. "https://commons-dss.ucsc-cgp-dev.org/v1"
        :param staging_bucket: The name of the AWS S3 bucket to be used when staging files for uploading
        to the DSS. As an example, local files are uploaded to the staging bucket, then file metadata tags
        required by the DSS are assigned to it, then the file is loaded into the DSS (by copy).
        The bucket must be accessible by the DSS. .e.g. 'commons-dss-upload'
        :param google_project_id: A Google `Project ID` to be used when accessing GCP requester pays buckets.
        e.g. "platform-dev-178517"
        One way to find a `Project ID` is provided here:
        https://console.cloud.google.com/cloud-resource-manager
        :param dry_run: If True, log the actions that would be performed yet don't actually execute them.
        Otherwise, actually perform the operations.
        """
        self.dss_endpoint = dss_endpoint
        self.staging_bucket = staging_bucket
        self.google_project_id = google_project_id
        self.dry_run = dry_run
        self.s3_client = boto3.client("s3")
        self.blobstore = s3.S3BlobStore(self.s3_client)
        self.gs_client = Client()
        os.environ.pop('HCA_CONFIG_FILE', None)
        self.dss_client = DSSClient()
        self.dss_client.host = self.dss_endpoint

    def upload_cloud_file(self, bucket: str, key: str, bundle_uuid: str, file_uuid: str) -> tuple:
        """
        Upload a file contained in an S3 bucket into a given DSS by copy.
        If the given cloud file does not have the file metadata tags required
        by the DSS, checksums for the file are calculated and tagged to the
        source file in place. Then the file is uploaded to the DSS by copy.

        This differs from the typical practice of copying the source cloud file
        to a staging bucket, tagging the file there, then uploading it to the DSS.
        This method was provided for use case of loading large (e.g. 20GB) data files,
        from a bucket in which the caller has permission to assign tags, into the DSS by copy.
        This was the scenario when originally loading the TOPMed 107 open access files
        into the DSS by copy. This method should not be used for loading large datasets
        from controlled access buckets.

        NOTE: S3 ONLY!

        :param bucket: Name of an S3 bucket containing the cloud file to be loaded.
        :param key: S3 file to upload.  e.g. 'output.txt' or 'data/output.txt'
        :param bundle_uuid: An RFC4122-compliant UUID to be used to identify the bundle containing the file
        :param file_uuid: An RFC4122-compliant UUID to be used to identify the file
        :return: file_uuid: str, file_version: str, filename: str
        """
        if not self._has_hca_tags(self.blobstore, bucket, key):
            checksums = self._calculate_checksums(self.s3_client, bucket, key)
            self._set_hca_metadata_tags(self.s3_client, bucket, key, checksums)
        return self._upload_tagged_cloud_file_to_dss(bucket, key, file_uuid, bundle_uuid)

    def upload_cloud_file_by_reference(self,
                                       filename: str,
                                       file_uuid: str,
                                       file_cloud_urls: set,
                                       bundle_uuid: str,
                                       guid: str,
                                       file_version: str=None) -> tuple:
        """
        Loads the given cloud file into the DSS by reference, rather than by copying it into the DSS.
        Because the HCA DSS per se does not support loading by reference, this is currently implemented
        using the approach described here:
        https://docs.google.com/document/d/1QSa7Ubw-muyD_u0X_dq9WeKyK_dCJXi4Ex7S_pil1uk/edit#heading=h.exnqjy2n2q78

        This is conceptually similar to creating a "symbolic link" to the cloud file rather than copying the
        source file into the DSS.
        The file's metadata is obtained, formatted as a dictionary, then this dictionary is uploaded as
        as a json file with content type `dss-type=fileref` into the DSS.

        A request has been made for the HCA data-store to support loading by reference as a feature of the
        data store, here: https://github.com/HumanCellAtlas/data-store/issues/912

        :param filename: The name of the file in the bucket.
        :param file_uuid: An RFC4122-compliant UUID to be used to identify the file
        :param file_cloud_urls: A set of 'gs://' and 's3://' bucket links.
                                e.g. {'gs://broad-public-datasets/g.bam', 's3://ucsc-topmed-datasets/a.bam'}
        :param bundle_uuid: n RFC4122-compliant UUID to be used to identify the bundle containing the file
        :param guid: An optional additional/alternate data identifier/alias to associate with the file
        e.g. "dg.4503/887388d7-a974-4259-86af-f5305172363d"
        :param file_version: a RFC3339 compliant datetime string
        :return: file_uuid: str, file_version: str, filename: str
        """
        if self.dry_run:
            logger.info(f"DRY RUN: upload_cloud_file_by_reference: {filename} {str(file_cloud_urls)} {bundle_uuid}")

        file_reference = self._create_file_reference(file_cloud_urls, guid)
        return self.upload_dict_as_file(file_reference,
                                        filename,
                                        file_uuid,
                                        bundle_uuid,
                                        file_version=file_version,
                                        content_type="application/json; dss-type=fileref")

    def _create_file_reference(self, file_cloud_urls: set, guid: str) -> dict:
        """
        Format a file's metadata into a dictionary for uploading as a json to support the approach
        described here:
        https://docs.google.com/document/d/1QSa7Ubw-muyD_u0X_dq9WeKyK_dCJXi4Ex7S_pil1uk/edit#heading=h.exnqjy2n2q78

        :param file_cloud_urls: A set of 'gs://' and 's3://' bucket links.
                                e.g. {'gs://broad-public-datasets/g.bam', 's3://ucsc-topmed-datasets/a.bam'}
        :param guid: An optional additional/alternate data identifier/alias to associate with the file
        e.g. "dg.4503/887388d7-a974-4259-86af-f5305172363d"
        :param file_version: RFC3339 formatted timestamp.
        :return: A dictionary of metadata values.
        """
        s3_metadata = None
        gs_metadata = None
        for cloud_url in file_cloud_urls:
            url = urlparse(cloud_url)
            bucket = url.netloc
            key = url.path[1:]
            if url.scheme == "s3":
                s3_metadata = self._get_s3_file_metadata(bucket, key)
            elif url.scheme == "gs":
                gs_metadata = self._get_gs_file_metadata(bucket, key)
            else:
                logger.warning("Unsupported cloud URL scheme: {cloud_url}")
        return self._consolidate_metadata(file_cloud_urls, s3_metadata, gs_metadata, guid)

    def _get_s3_file_metadata(self, bucket: str, key: str) -> dict:
        """
        Format an S3 file's metadata into a dictionary for uploading as a json.

        :param bucket: Name of an S3 bucket
        :param key: S3 file to upload.  e.g. 'output.txt' or 'data/output.txt'
        :return: A dictionary of metadata values.
        """
        metadata = dict()
        try:
            response = self.s3_client.head_object(Bucket=bucket, Key=key, RequestPayer="requester")
            metadata['content-type'] = response['ContentType']
            metadata['s3_etag'] = response['ETag']
            metadata['size'] = response['ContentLength']
        except botocore.exceptions.ClientError as e:
            logger.warning(f"Error accessing s3://{bucket}/{key} Exception: {e}")
        return metadata

    def _get_gs_file_metadata(self, bucket: str, key: str) -> dict:
        """
        Format a GS file's metadata into a dictionary for uploading as a JSON file.

        :param bucket: Name of a GS bucket.
        :param key: GS file to upload.  e.g. 'output.txt' or 'data/output.txt'
        :return: A dictionary of metadata values.
        """
        metadata = dict()
        try:
            gs_bucket = self.gs_client.bucket(bucket, self.google_project_id)
            blob_obj = gs_bucket.get_blob(key)
            metadata['content-type'] = blob_obj.content_type
            metadata['crc32c'] = binascii.hexlify(base64.b64decode(blob_obj.crc32c)).decode("utf-8").lower()
            metadata['size'] = blob_obj.size
        except Exception as e:
            logger.warning(f"Error accessing gs://{bucket}/{key} Exception: {e}")
        return metadata

    @staticmethod
    def _consolidate_metadata(file_cloud_urls: set,
                              s3_metadata: Optional[Dict[str, Any]],
                              gs_metadata: Optional[Dict[str, Any]],
                              guid: str) -> dict:
        """
        Consolidates cloud file metadata to create the JSON used to load by reference
        into the DSS.

        :param file_cloud_urls: A set of 'gs://' and 's3://' bucket URLs.
                                e.g. {'gs://broad-public-datasets/g.bam', 's3://ucsc-topmed-datasets/a.bam'}
        :param s3_metadata: Dictionary of meta data produced by _get_s3_file_metadata().
        :param gs_metadata: Dictionary of meta data produced by _get_gs_file_metadata().
        :param guid: An optional additional/alternate data identifier/alias to associate with the file
        e.g. "dg.4503/887388d7-a974-4259-86af-f5305172363d"
        :return: A dictionary of cloud file metadata values
        """
        consolidated_metadata = dict()
        if s3_metadata:
            consolidated_metadata.update(s3_metadata)
        if gs_metadata:
            consolidated_metadata.update(gs_metadata)
        consolidated_metadata['url'] = list(file_cloud_urls)
        # TODO double check aliases
        consolidated_metadata['aliases'] = [str(guid)]
        return consolidated_metadata

    def upload_dict_as_file(self, value: dict,
                            filename: str,
                            file_uuid: str,
                            bundle_uuid: str,
                            file_version: str=None,  # RFC3339
                            content_type=None):
        """
        Create a JSON file in the DSS containing the given dict.

        :param value: A dictionary representing the JSON content of the file to be created.
        :param filename: The basename of the file in the bucket.
        :param file_uuid: An RFC4122-compliant UUID to be used to identify the file
        :param An RFC4122-compliant UUID to be used to identify the bundle containing the file
        :param content_type: Content description e.g. "application/json; dss-type=fileref".
        :param file_version: a RFC3339 compliant datetime string
        :return: file_uuid: str, file_version: str, filename: str
        """
        tempdir = mkdtemp()
        file_path = "/".join([tempdir, filename])
        with open(file_path, "w") as fh:
            fh.write(json.dumps(value, indent=4))
        result = self.upload_local_file(file_path,
                                        file_uuid,
                                        bundle_uuid,
                                        file_version=file_version,
                                        content_type=content_type)
        os.remove(file_path)
        os.rmdir(tempdir)
        return result

    def upload_local_file(self, path: str,
                          file_uuid: str,
                          bundle_uuid: str,
                          file_version: str=None,
                          content_type=None):
        """
        Upload a file from the local file system to the DSS.

        :param path: Path to a local file.
        :param file_uuid: An RFC4122-compliant UUID to be used to identify the file
        :param bundle_uuid: An RFC4122-compliant UUID to be used to identify the bundle containing the file
        :param content_type: Content type identifier, for example: "application/json; dss-type=fileref".
        :param file_version: a RFC3339 compliant datetime string
        :return: file_uuid: str, file_version: str, filename: str
        """
        file_uuid, key = self._upload_local_file_to_staging(path, file_uuid, content_type)
        return self._upload_tagged_cloud_file_to_dss(self.staging_bucket,
                                                     key,
                                                     file_uuid,
                                                     bundle_uuid,
                                                     file_version=file_version)

    def load_bundle(self, file_info_list: list, bundle_uuid: str):
        """
        Loads a bundle to the DSS that contains the specified files.

        :param file_info_list:
        :param bundle_uuid: An RFC4122-compliant UUID to be used to identify the bundle containing the file
        :return: A full qualified bundle id e.g. "{bundle_uuid}.{version}"
        """
        kwargs = dict(replica="aws", creator_uid=CREATOR_ID, files=file_info_list, uuid=bundle_uuid)
        if not self.dry_run:
            response = self.dss_client.put_bundle(**kwargs)
            version = response['version']
        else:
            logger.info("DRY RUN: DSS put bundle: " + str(kwargs))
            version = None
        bundle_fqid = f"{bundle_uuid}.{version}"
        logger.info(f"Loaded bundle: {bundle_fqid}")
        return bundle_fqid

    @staticmethod
    def get_filename_from_key(key: str):
        assert not key.endswith('/'), 'Please specify a filename, not a directory ({} cannot end in "/").'.format(key)
        return key.split("/")[-1]

    def _upload_local_file_to_staging(self, path: str, file_uuid: str, content_type):
        """
        Uploads a local file to the (S3) staging bucket then tags the file with the checksum
        values required to load into the DSS.

        :param path: Path to a local file.
        :param file_uuid: An RFC4122-compliant UUID to be used to identify the file.
        :param content_type: Content description, for example: "application/json; dss-type=fileref".
        :return: file_uuid: str, key_name: str
        """
        with open(path, "rb") as fh:
            file_uuid, key_name = upload_to_cloud(fh, file_uuid, self.staging_bucket, content_type)
        return file_uuid, key_name

    @staticmethod
    def _has_hca_tags(blobstore: BlobStore, bucket: str, key: str) -> bool:
        """
        Return True if all of the following tags are found:
            "hca-dss-s3_etag"
            "hca-dss-sha1"
            "hca-dss-sha256"
            "hca-dss-crc32c"

        :param blobstore: cloud.blobstore.BlobStore
        :param bucket: Name of an S3 bucket.  e.g. 'commons-dss-upload'
        :param key: s3 file to upload.  e.g. 'output.txt' or 'data/output.txt'
        :return: bool
        """
        hca_tag_names = {"hca-dss-s3_etag", "hca-dss-sha1", "hca-dss-sha256", "hca-dss-crc32c"}
        metadata = blobstore.get_user_metadata(bucket, key)
        return hca_tag_names.issubset(metadata.keys())

    @staticmethod
    def _calculate_checksums(s3_client, bucket: str, key: str) -> typing.Dict:
        """
        Creates checksums for a file in an S3 bucket.

        :param s3_client: An instance of boto3.client("s3").
        :param bucket: Name of an s3 bucket.  e.g. 'commons-dss-upload'
        :param key: s3 file to upload.  e.g. 'output.txt' or 'data/output.txt'
        :return: A dictionary of checksums
        """
        checksumming_sink = ChecksummingSink()
        tx_cfg = TransferConfig(multipart_threshold=S3Etag.etag_stride,
                                multipart_chunksize=S3Etag.etag_stride)
        s3_client.download_fileobj(bucket, key, checksumming_sink, Config=tx_cfg)
        return checksumming_sink.get_checksums()

    def _set_hca_metadata_tags(self, s3_client, bucket: str, key: str, checksums: dict) -> None:
        """
        Sets an HCA file metadata tag for each of the required checksums.

        :param s3_client: An instance of boto3.client("s3").
        :param bucket: Name of an S3 bucket.  e.g. 'commons-dss-upload'
        :param key: S3 file to upload.  e.g. 'output.txt' or 'data/output.txt'
        :param checksums: A dictionary of checksums.
        """
        metadata = {
            "hca-dss-s3_etag": checksums["s3_etag"],
            "hca-dss-sha1": checksums["sha1"],
            "hca-dss-sha256": checksums["sha256"],
            "hca-dss-crc32c": checksums["crc32c"]
        }
        s3_client.put_object_tagging(Bucket=bucket,
                                     Key=key,
                                     Tagging=dict(TagSet=encode_tags(metadata))
                                     )

    def _upload_tagged_cloud_file_to_dss(self, source_bucket: str,
                                         source_key: str,
                                         file_uuid: str,
                                         bundle_uuid: str,
                                         file_version: str=None,
                                         timeout_seconds=1200):
        """
        Uploads a tagged file contained in a cloud bucket to the DSS.

        :param source_bucket: Name of an S3 bucket.  e.g. 'commons-dss-upload'
        :param source_key: S3 file to upload.  e.g. 'output.txt' or 'data/output.txt'
        :param file_uuid: An RFC4122-compliant UUID to be used to identify the file.
        :param bundle_uuid: An RFC4122-compliant UUID to be used to identify the bundle containing the file
        :param file_version: a RFC3339 compliant datetime string
        :param timeout_seconds:  Amount of time to continue attempting an async copy.
        :return: file_uuid: str, file_version: str, filename: str
        """
        source_url = f"s3://{source_bucket}/{source_key}"
        filename = self.get_filename_from_key(source_key)

        if self.dry_run:
            logger.info(
                f"DRY RUN: _upload_tagged_cloud_file_to_dss: {source_bucket} {source_key} {file_uuid} {bundle_uuid}")
            return file_uuid, file_version, filename

        request_parameters = dict(uuid=file_uuid, version=file_version, bundle_uuid=bundle_uuid, creator_uid=CREATOR_ID,
                                  source_url=source_url)
        if self.dry_run:
            print("DRY RUN: put file: " + str(request_parameters))
            return file_uuid, file_version, filename

        copy_start_time = time.time()
        response = self.dss_client.put_file._request(request_parameters)

        # the version we get back here is formatted in the way DSS likes
        # and we need this format update when doing load bundle
        file_version = response.json().get('version', "blank")

        if response.status_code in (requests.codes.ok, requests.codes.created):
            logger.info("File %s: Sync copy -> %s (%d seconds)",
                        source_url, file_version, (time.time() - copy_start_time))
        else:
            assert response.status_code == requests.codes.accepted
            logger.info("File %s: Starting async copy -> %s", source_url, file_version)

            timeout = time.time() + timeout_seconds
            wait = 1.0
            while time.time() < timeout:
                try:
                    self.dss_client.head_file(uuid=file_uuid, replica="aws", version=file_version)
                    logger.info("File %s: Finished async copy -> %s (approximately %d seconds)",
                                source_url, file_version, (time.time() - copy_start_time))
                    break
                except SwaggerAPIException as e:
                    if e.code != requests.codes.not_found:
                        msg = "File {}: Unexpected server response during registration"
                        raise RuntimeError(msg.format(source_url))
                    time.sleep(wait)
                    wait = min(10.0, wait * self.dss_client.UPLOAD_BACKOFF_FACTOR)
            else:
                # timed out. :(
                raise RuntimeError("File {}: registration FAILED".format(source_url))
            logger.debug("Successfully uploaded file")

        return file_uuid, file_version, filename


class MetadataFileUploader:
    def __init__(self, dss_uploader: DssUploader) -> None:
        self.dss_uploader = dss_uploader

    def load_cloud_file(self, bucket: str, key: str, filename: str, schema_url: str, bundle_uuid: str) -> tuple:
        metadata_string = self.dss_uploader.blobstore.get(bucket, key).decode("utf-8")
        metadata = json.loads(metadata_string)
        return self.load_dict(metadata, filename, schema_url, bundle_uuid)

    def load_local_file(self, local_filename: str, filename: str, schema_url: str, bundle_uuid: str) -> tuple:
        with open(local_filename, "r") as fh:
            metadata = json.load(fh)
        return self.load_dict(metadata, filename, schema_url, bundle_uuid)

    def load_dict(self, metadata: dict, filename: str, schema_url: str, bundle_uuid: str) -> tuple:
        metadata['describedBy'] = schema_url
        return self.dss_uploader.upload_dict_as_file(metadata, filename, str(uuid.uuid4()), bundle_uuid)

def load_json_from_file(input_file_path: str) -> dict:
    with open(input_file_path) as fh:
        return json.load(fh)

def suppress_verbose_logging():
    for logger_name in logging.Logger.manager.loggerDict:  # type: ignore
        if (logger_name.startswith("botocore") or
                logger_name.startswith("boto3.resources")):
            logging.getLogger(logger_name).setLevel(logging.WARNING)
