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
from io import open
from tempfile import mkdtemp
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
        if not self._has_hca_tags(self.blobstore, bucket, key):
            checksums = self._calculate_checksums(self.s3_client, bucket, key)
            self._set_hca_metadata_tags(self.s3_client, bucket, key, checksums)
        return self._upload_tagged_cloud_file_to_dss(bucket, key, file_uuid, bundle_uuid)

    def upload_cloud_file_by_reference(self, filename: str, file_cloud_urls: set, bundle_uuid: str, file_uuid: str) -> tuple:
        if self.dry_run:
            logger.info(f"DRY RUN: upload_cloud_file_by_reference: {filename} {str(file_cloud_urls)} {bundle_uuid}")
        file_reference = self._create_file_reference(file_cloud_urls)
        return self.upload_dict_as_file(file_reference, filename, bundle_uuid, "application/json; dss-type=fileref")

    def _create_file_reference(self, file_cloud_urls: set) -> dict:
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
        return self._consolidate_metadata(file_cloud_urls, s3_metadata, gs_metadata)

    def _get_s3_file_metadata(self, bucket: str, key: str) -> dict:
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
    def _consolidate_metadata(file_cloud_urls: set, s3_metadata: dict, gs_metadata: dict) -> dict:
        consolidated_metadata = dict()
        if s3_metadata:
            consolidated_metadata.update(s3_metadata)
        if gs_metadata:
            consolidated_metadata.update(gs_metadata)
        consolidated_metadata['url'] = list(file_cloud_urls)
        return consolidated_metadata

    def upload_dict_as_file(self, value: dict, filename: str, bundle_uuid: str, content_type=None):
        tempdir = mkdtemp()
        file_path = "/".join([tempdir, filename])
        with open(file_path, "w") as fh:
            fh.write(json.dumps(value, indent=4))
        result = self.upload_local_file(file_path, bundle_uuid, content_type)
        os.remove(file_path)
        os.rmdir(tempdir)
        return result

    # TODO Add ability to specify file_uuid
    def upload_local_file(self, path: str, bundle_uuid: str, content_type=None):
        file_uuid, key = self._upload_local_file_to_staging(path, content_type)
        return self._upload_tagged_cloud_file_to_dss(self.staging_bucket, key, file_uuid, bundle_uuid)

    def load_bundle(self, file_info_list: list, bundle_uuid: str):
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
        return key.split("/")[-1]

    # TODO Add ability to specify file_uuid
    def _upload_local_file_to_staging(self, path: str, content_type):
        with open(path, "rb") as fh:
            file_uuids, key_names = upload_to_cloud([fh], self.staging_bucket, "aws", False, content_type)
        return file_uuids[0], key_names[0]

    @staticmethod
    def _has_hca_tags(blobstore: BlobStore, bucket: str, key: str) -> bool:
        hca_tag_names = {"hca-dss-s3_etag", "hca-dss-sha1", "hca-dss-sha256", "hca-dss-crc32c"}
        metadata = blobstore.get_user_metadata(bucket, key)
        return hca_tag_names.issubset(metadata.keys())

    @staticmethod
    def _calculate_checksums(s3_client, bucket: str, key: str) -> typing.Dict:
        checksumming_sink = ChecksummingSink()
        tx_cfg = TransferConfig(multipart_threshold=S3Etag.etag_stride,
                                multipart_chunksize=S3Etag.etag_stride)
        s3_client.download_fileobj(bucket, key, checksumming_sink, Config=tx_cfg)
        return checksumming_sink.get_checksums()

    def _set_hca_metadata_tags(self, s3_client, bucket: str, key: str, checksums: dict) -> None:
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

    def _upload_tagged_cloud_file_to_dss(self, source_bucket: str, source_key: str, file_uuid: str, bundle_uuid: str,
                                         timeout_seconds=1200):
        source_url = f"s3://{source_bucket}/{source_key}"
        filename = self.get_filename_from_key(source_key)

        if self.dry_run:
            logger.info(
                f"DRY RUN: _upload_tagged_cloud_file_to_dss: {source_bucket} {source_key} {file_uuid} {bundle_uuid}")
            return file_uuid, None, filename

        request_parameters = dict(uuid=file_uuid, bundle_uuid=bundle_uuid, creator_uid=CREATOR_ID,
                                  source_url=source_url)
        if self.dry_run:
            print("DRY RUN: put file: " + str(request_parameters))
            return file_uuid, None, filename

        copy_start_time = time.time()
        response = self.dss_client.put_file._request(request_parameters)
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
        return self.dss_uploader.upload_dict_as_file(metadata, filename, bundle_uuid)

def load_json_from_file(input_file_path: str) -> dict:
        with open(input_file_path) as fh:
            return json.load(fh)

def suppress_verbose_logging():
    for logger_name in logging.Logger.manager.loggerDict:  # type: ignore
        if (logger_name.startswith("botocore") or
                logger_name.startswith("boto3.resources")):
            logging.getLogger(logger_name).setLevel(logging.WARNING)
