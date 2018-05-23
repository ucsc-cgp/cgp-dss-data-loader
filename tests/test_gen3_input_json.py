import os
import subprocess
import unittest
import hca
import uuid
import json
import time
import requests

import git


class TestBasicInputFormatLoading(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = hca.dss.DSSClient()
        cls.client.host = 'https://hca-dss-4.ucsc-cgp-dev.org/v1'
        cls.dss_endpoint = os.getenv('TEST_DSS_ENDPOINT', cls.client.host)
        cls.staging_bucket = os.getenv('DSS_S3_STAGING_BUCKET', 'mbaumann-dss-staging')
        cls.git_repo = git.Repo(os.getcwd(), search_parent_directories=True)
        cls.project_path = cls.git_repo.git.rev_parse('--show-toplevel')

    def test_gen3_input_format_loading_from_cli(self, sample_input='/tests/test_data/gen3_sample_input.json'):
        '''
        Test that a gen3 formatted json can be uploaded to the DSS, and that all of
        the files loaded are only loaded by reference (and also are not indexed).

        1. Generates a json from a template with a new unique 'bundle_did'
           and a new 'did' for the first file in the bundle.
        2. Searches the DSS to make sure it doesn't already exist using the HCA CLI.
        3. Uploads the gen3 json to the DSS.
        4. Searches the DSS to make sure it now exists and the upload was successful.
        5. Assert files are loaded by reference (and also are not indexed).
        6. Assert that the new 'did' for the first file in the bundle was found in the results.
        '''
        # gen3 json template to use
        json_input_file = f'{self.project_path}' + sample_input
        # modified json_input_file with the template 'bundle_did' overwritten to a fresh one
        tmp_json = f'{self.project_path}/tests/test_data/tmp.json'

        # mint a new 'bundle_did'
        guid = str(uuid.uuid4())
        # make new guid for first file
        file_guid = str(uuid.uuid4())

        # copy the contents of json_input_file to tmp_json
        # but change 'bundle_did' to a new guid
        with open(json_input_file, 'r') as jsonFile:
            json_contents = json.load(jsonFile)
        json_contents[0]['bundle_did'] = guid
        json_contents[0]['manifest'][0]['did'] = file_guid
        with open(tmp_json, 'w') as jsonFile:
            json.dump(json_contents, jsonFile)

        # search for the newly minted guid in the DSS to make sure it does not exist yet
        res = self.client.post_search(es_query={'query': {'term': {'uuid': guid}}}, replica='aws')
        assert res['total_hits'] == 0

        # upload the data bundle to the DSS
        run(f'{self.project_path}/scripts/cgp_data_loader.py --no-dry-run --dss-endpoint {self.dss_endpoint}'
            f' --staging-bucket {self.staging_bucket}'
            f' gen3 --json-input-file {tmp_json}')

        time.sleep(5) # there is some lag in uploading

        # search for the newly minted guid in the DSS and make sure it now exists and uploading was successful
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
        os.remove(tmp_json)

    def test_gen3_input2_format_loading_from_cli(self):
        self.test_gen3_input_format_loading_from_cli(sample_input='/tests/test_data/gen3_sample_input2.json')

    def test_known_guid_exists(self):
        '''
        Test to make sure 1+ search results are found for an existent guid in the DSS.
        '''
        guid = 'a47b90b2-0967-4fbf-87bc-c6c12db3fedf'
        res = self.client.post_search(es_query={'query': {'term': {'uuid': guid}}}, replica='aws')
        assert res['total_hits'] > 0

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
