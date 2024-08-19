PIP_COMPILE := pipx run uv pip compile pyproject.toml --quiet

ifeq ($(filter upgrade,$(MAKECMDGOALS)),upgrade)
PIP_COMPILE += --upgrade
endif

.PHONY: all base uv upgrade

all: base

base:
	$(PIP_COMPILE) --output-file=requirements/requirements.txt
