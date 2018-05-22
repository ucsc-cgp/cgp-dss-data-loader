import uuid

from loader.base_loader import DssUploader, MetadataFileUploader

SCHEMA_URL = ('https://raw.githubusercontent.com/DataBiosphere/metadata-schema/master'
              '/json_schema/cgp/gen3/0.1.0/cgp_gen3_metadata.json')


class Gen3FormatBundleUploader:
    def __init__(self, dss_uploader: DssUploader, metadata_file_uploader: MetadataFileUploader) -> None:
        self.dss_uploader = dss_uploader
        self.metadata_file_uploader = metadata_file_uploader

    @staticmethod
    def _parse_bundle(bundle: dict) -> tuple:
        try:
            bundle_uuid = bundle['bundle_did']
        except KeyError:
            bundle_uuid = uuid.uuid4()
        try:
            metadata_dict = bundle['metadata']
        except KeyError:
            metadata_dict = {key: bundle[key] for key in ['aliquot', 'sample', 'core_metadata']}
        file_manifest = bundle['manifest']
        return bundle_uuid, metadata_dict, file_manifest

    def _load_bundle(self, bundle: dict):
        bundle_uuid, metadata_dict, file_manifest = self._parse_bundle(bundle)
        file_info_list = []

        # load metadata
        file_uuid, file_version, filename = \
            self.metadata_file_uploader.load_dict(metadata_dict,
                                                  "metadata.json",
                                                  SCHEMA_URL,
                                                  bundle_uuid)
        file_info_list.append(dict(uuid=file_uuid, version=file_version, name=filename, indexed=True))

        # load data files by reference
        for file_info in file_manifest:
            cloud_urls = {file_info[key] for key in ['s3url', 'gsurl']}
            file_uuid, file_version, filename = \
                self.dss_uploader.upload_cloud_file_by_reference(file_info['name'],
                                                                 # use did for uuid for now. will probably have to
                                                                 # extract from guid (did) in the future
                                                                 file_info['did'],
                                                                 cloud_urls,
                                                                 bundle_uuid,
                                                                 file_info['did'])
            file_info_list.append(dict(uuid=file_uuid, version=file_version, name=filename, indexed=False))

        # load bundle
        self.dss_uploader.load_bundle(file_info_list, bundle_uuid)

    def load_all_bundles(self, input_json: list):
        for bundle in input_json:
            self._load_bundle(bundle)
