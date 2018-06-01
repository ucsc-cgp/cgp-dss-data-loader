import warnings


def ignore_resource_warnings(test_func):
    # see https://stackoverflow.com/q/26563711/7830612 for justification
    def do_test(self, *args, **kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ResourceWarning)
            test_func(self, *args, **kwargs)

    return do_test
