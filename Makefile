
include common.mk
MODULES=loader tests

all: test

lint:
	flake8 $(MODULES)

mypy:
	mypy --ignore-missing-imports $(MODULES)

tests:=$(wildcard tests/test_*.py)

# A pattern rule that runs a single test script
#
$(tests): %.py : mypy lint
	python -m unittest $*.py

test: $(tests)

.PHONY: all lint mypy test

