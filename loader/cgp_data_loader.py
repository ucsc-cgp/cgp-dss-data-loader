import logging
import sys

import os

from loader.base_loader import DssUploader, MetadataFileUploader, suppress_verbose_logging

DSS_ENDPOINT_DEFAULT = "https://commons-dss.ucsc-cgp-dev.org/v1"
STAGING_BUCKET_DEFAULT = "commons-dss-staging"

# Google Cloud Access
# TODO Make GOOGLE_PROJECT_ID configurable via a command-line option
GOOGLE_PROJECT_ID = "platform-dev-178517"  # For requester pays buckets

# TODO Get the schema URL from the input data
SCHEMA_URL_DEFAULT = ("https://raw.githubusercontent.com/DataBiosphere/commons-sample-data/master"
                      "/json_schema/spinnaker_metadata/1.2.1/spinnaker_metadata_schema.json")


def main(argv):
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true",
                       help="Output actions that would otherwise be performed.")
    group.add_argument("--no-dry-run", dest="dry_run", action="store_false",
                       help="Perform the actions.")
    parser.add_argument("--dss-endpoint", metavar="DSS_ENDPOINT", required=False,
                        default=DSS_ENDPOINT_DEFAULT,
                        help="The HCA Data Storage System endpoint to use.")
    parser.add_argument("--staging-bucket", metavar="STAGING_BUCKET", required=False,
                        default=STAGING_BUCKET_DEFAULT,
                        help="The bucket to stage local files for uploading to DSS.")

    options = parser.parse_args(argv)

    # Clear configured credentials, which are likely for service accounts.
    os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    os.environ.pop('GOOGLE_APPLICATION_SECRETS', None)

    dss_uploader = DssUploader(options.dss_endpoint, options.staging_bucket, options.dry_run)
    metadata_file_uploader = MetadataFileUploader(dss_uploader)

    # TODO Perform loading!

    if __name__ == '__main__':
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        suppress_verbose_logging()
        main(sys.argv[1:])
