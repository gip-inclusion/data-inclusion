# must match the python3 of the debian release in the Dockerfile base image
# (trixie ships 3.13), used by plpython3u and the venv
PYTHON_VERSION := 3.13

UV_COMPILE := uv pip compile --quiet --python-version $(PYTHON_VERSION)

# `make upgrade all` recompiles with --upgrade
ifeq ($(filter upgrade,$(MAKECMDGOALS)),upgrade)
UV_COMPILE += --upgrade
endif

# always regenerated: git does not preserve mtimes
.PHONY: all upgrade requirements.txt

all: requirements.txt

requirements.txt: requirements.in
	$(UV_COMPILE) $< --output-file=$@
