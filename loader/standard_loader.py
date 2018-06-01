from loader.base_loader import DssUploader, MetadataFileUploader


class StandardFormatBundleUploader:
    def __init__(self, dss_uploader: DssUploader, metadata_file_uploader: MetadataFileUploader) -> None:
        self.dss_uploader = dss_uploader
        self.metadata_file_uploader = metadata_file_uploader

    def load_all_bundles(self, input_json: dict):
        # TODO Implement this loader
        raise NotImplemented
