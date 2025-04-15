PIP_COMPILE := uv pip compile pyproject.toml --quiet

ifeq ($(filter upgrade,$(MAKECMDGOALS)),upgrade)
PIP_COMPILE += --upgrade
endif

.PHONY: all dev base test uv upgrade

all: base dev test

base:
	$(PIP_COMPILE) --output-file=requirements/requirements.txt

dev:
	$(PIP_COMPILE) --extra=dev --output-file=requirements/dev-requirements.txt

test:
	$(PIP_COMPILE) --extra=test --output-file=requirements/test-requirements.txt
