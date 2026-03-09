PYTHON_VERSION := 3.12

PIP_COMPILE := uv pip compile --quiet --python-version $(PYTHON_VERSION)

ifeq ($(filter upgrade,$(MAKECMDGOALS)),upgrade)
PIP_COMPILE += --upgrade
endif

.PHONY: all tasks dev upgrade

all: tasks dev

tasks: tasks/requirements.in
	$(PIP_COMPILE) tasks/requirements.in --output-file=tasks/requirements.txt

dev: tasks dev/requirements.in
	$(PIP_COMPILE) dev/requirements.in --output-file=dev/requirements.txt
