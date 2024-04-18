UV_VERSION := 0.1.33

PIP_COMPILE := pipx run uv pip compile setup.py --quiet

ifeq ($(filter upgrade,$(MAKECMDGOALS)),upgrade)
PIP_COMPILE += --upgrade
endif

.PHONY: all dev base test uv upgrade

all: base dev test

uv:
	pip install uv==$(UV_VERSION)

base: uv
	$(PIP_COMPILE) --output-file=requirements/requirements.txt

dev: uv
	$(PIP_COMPILE) --extra=dev --output-file=requirements/dev-requirements.txt

test: uv
	$(PIP_COMPILE) --extra=test --output-file=requirements/test-requirements.txt
