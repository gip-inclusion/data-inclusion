# `datawarehouse`

Custom postgresql image with `plpython3u` enabled.

`plpython3u` is used to embed python transformations within dbt models.

It is generally considered unsafe therefore not available on managed databases.

## compiling the python requirements

The base image is based on debian `trixie` which ships with python3.13.

For the sake of simplicity, this will be the target python version.

The `requirements.txt` should therefore be compiled in a python3.13 environment.

```bash
uv pip compile --python-version 3.13 requirements.in --output-file requirements.txt
```
