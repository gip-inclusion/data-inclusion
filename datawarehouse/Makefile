# must match the python3 of the debian release in the Dockerfile base image
# (trixie ships 3.13), used by plpython3u and the venv
PYTHON_VERSION := 3.13

# resolve platform markers for the image, not the host (e.g. darwin-only deps on macos):
# x86_64 like the image builds and servers, glibc 2.41 on trixie (2.40 is uv's closest)
PYTHON_PLATFORM := x86_64-manylinux_2_40

UV_COMPILE := uv pip compile --quiet --python-version $(PYTHON_VERSION) --python-platform $(PYTHON_PLATFORM)

# `make upgrade` recompiles with --upgrade
ifeq ($(filter upgrade,$(MAKECMDGOALS)),upgrade)
UV_COMPILE += --upgrade
endif

# always regenerated: git does not preserve mtimes
.PHONY: all upgrade requirements.txt

all: requirements.txt

upgrade: requirements.txt

requirements.txt: requirements.in
	$(UV_COMPILE) $< --output-file=$@
