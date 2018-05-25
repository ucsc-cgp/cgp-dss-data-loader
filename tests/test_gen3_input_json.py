import datetime
import os
import subprocess
import tempfile
import unittest
import warnings
from contextlib import contextmanager

import hca
import uuid
import json
import time
import requests

import git

from scripts.cgp_data_loader import main


def ignore_resource_warnings(test_func):
    # see https://stackoverflow.com/q/26563711/7830612 for justification
    def do_test(self, *args, **kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ResourceWarning)
            test_func(self, *args, **kwargs)
    return do_test


class TestGen3InputFormatLoading(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = hca.dss.DSSClient()
        cls.client.host = 'https://hca-dss-4.ucsc-cgp-dev.org/v1'
        cls.dss_endpoint = os.getenv('TEST_DSS_ENDPOINT', cls.client.host)
        cls.staging_bucket = os.getenv('DSS_S3_STAGING_BUCKET', 'mbaumann-dss-staging')
        cls.git_repo = git.Repo(os.getcwd(), search_parent_directories=True)
        cls.project_path = cls.git_repo.git.rev_parse('--show-toplevel')
        cls.test_files = [f'{cls.project_path}/tests/test_data/{test_file}'
                          for test_file in ['gen3_sample_input.json', 'gen3_sample_input2.json']]

    @staticmethod
    @contextmanager
    def _tmp_json_file(json_input_file, guid, file_guid, file_version):
        # copy the contents of json_input_file to tmp_json
        # but change 'bundle_did' to a new guid
        with open(json_input_file, 'r') as jsonFile:
            json_contents = json.load(jsonFile)
        json_contents[0]['bundle_did'] = guid
        json_contents[0]['manifest'][0]['did'] = file_guid
        for file_info in json_contents[0]['manifest']:
            file_info['updated_datetime'] = file_version
        with tempfile.NamedTemporaryFile() as jsonFile:
            with open(jsonFile.name, 'w') as fh:
                json.dump(json_contents, fh)
            yield jsonFile.name

    def _load_file(self, tmp_json):
        """run the load script and clean up after ourselves"""
        # upload the data bundle to the DSS
        args = ['--no-dry-run',
                '--dss-endpoint',
                f'{self.dss_endpoint}',
                '--staging-bucket',
                f'{self.staging_bucket}',
                'gen3',
                '--json-input-file',
                f'{tmp_json}']
        main(args)

    def test_gen3_input_format_loading_from_cli(self):
        self._test_gen3_input_format_loading_from_cli(self.test_files[0])

    def test_gen3_input_format2_loading_from_cli(self):
        self._test_gen3_input_format_loading_from_cli(self.test_files[1])

    @ignore_resource_warnings
    def _test_gen3_input_format_loading_from_cli(self, test_json):
        """
        Test that a gen3 formatted json can be uploaded to the DSS, and that all of
        the files loaded are only loaded by reference (and also are not indexed).

        1. Generates a json from a template with a new unique 'bundle_did'
           and a new 'did' for the first file in the bundle.
        2. Searches the DSS to make sure it doesn't already exist using the HCA CLI.
        3. Uploads the gen3 json to the DSS.
        4. Searches the DSS to make sure it now exists and the upload was successful.
        5. Assert files are loaded by reference (and also are not indexed).
        6. Assert that the new 'did' for the first file in the bundle was found in the results.
        """
        # mint a new 'bundle_did'
        guid = str(uuid.uuid4())
        # make new guid for first file
        file_guid = str(uuid.uuid4())
        # we want a new version of the file to be uploaded
        file_version = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self._test_gen3_loading(test_json, guid, file_guid, file_version)

        # test that uploading again will be handled successfully
        guid = str(uuid.uuid4())
        self._test_gen3_loading(test_json, guid, file_guid, file_version)

    def _test_gen3_loading(self, test_json, guid, file_guid, file_version):
        # search for the guid in the DSS to make sure it does not exist yet
        res = self.client.post_search(es_query={'query': {'term': {'uuid': guid}}}, replica='aws')
        assert res['total_hits'] == 0

        with self._tmp_json_file(test_json, guid, file_guid, file_version) as tmp_json:
            self._load_file(tmp_json)
            time.sleep(5)  # there is some lag in uploading

            # search for the guid in the DSS and make sure it now exists and uploading was successful
            res = self.client.post_search(es_query={'query': {'term': {'uuid': guid}}}, replica='aws')
            assert res['total_hits'] > 0

            # verify that all of the results (except metadata.json) are file references and not indexed
            found_matching_file = False
            for r in res['results']:
                response = requests.get(r['bundle_url'])
                returned_json = response.json()
                for f in returned_json['bundle']['files']:
                    if f['name'] != 'metadata.json':
                        assert f['indexed'] is False
                        assert 'dss-type=fileref' in f['content-type']

                        # verify that the file guid is stored
                        file_ref_json = self.client.get_file(uuid=f['uuid'], version=f['version'], replica='aws')
                        found_matching_file = found_matching_file or file_ref_json['aliases'][0] == file_guid
            assert found_matching_file

    def test_guid_missing(self):
        '''
        Test to make sure 0 search results are found for a non-existent guid in the DSS.
        '''
        guid = '73b12502-f60c-5f81-bd35-b0afc4678759'
        res = self.client.post_search(es_query={'query': {'term': {'uuid': guid}}}, replica='aws')
        assert res['total_hits'] == 0


def run(command, **kwargs):
    print(command)
    try:
        return subprocess.run(command, check=True, shell=isinstance(command, str), **kwargs)
    except subprocess.CalledProcessError as e:
        raise AssertionError(f'Exit status {e.returncode} while running "{command}".')


if __name__ == '__main__':
    unittest.main()
