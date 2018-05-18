SHELL=/bin/bash

ifeq ($(findstring Python 3.6, $(shell python --version 2>&1)),)
$(error Please run make commands from a Python 3.6 virtualenv)
endif