import os
import subprocess
import unittest

import git


class TestBasicInputFormatLoading(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.dss_endpoint = os.getenv("TEST_DSS_ENDPOINT", "https://hca-dss-4.ucsc-cgp-dev.org/")
        cls.staging_bucket = os.getenv("DSS_S3_STAGING_BUCKET", "mbaumann-dss-staging")
        cls.git_repo = git.Repo(os.getcwd(), search_parent_directories=True)
        cls.project_path = cls.git_repo.git.rev_parse("--show-toplevel")

    def test_basic_input_format_loading_from_cli(self):
        json_input_file = f"{self.project_path}/tests/test_data/basic_sample_input.json"

        run(f"{self.project_path}/scripts/cgp_data_loader.py --no-dry-run --dss-endpoint {self.dss_endpoint}"
            f" --staging-bucket {self.staging_bucket}"
            f" basic --json-input-file {json_input_file}")


def run(command, **kwargs):
    print(command)
    try:
        return subprocess.run(command, check=True, shell=isinstance(command, str), **kwargs)
    except subprocess.CalledProcessError as e:
        raise AssertionError(f'Exit status {e.returncode} while running "{command}".')


if __name__ == '__main__':
    unittest.main()
