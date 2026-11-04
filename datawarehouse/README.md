# `datawarehouse`

Custom postgresql image with `plpython3u` enabled.

`plpython3u` is used to embed python transformations within dbt models.

It is generally considered unsafe therefore not available on managed databases.

## compiling the python requirements

The base image is based on debian `bookworm` which ships with python3.11.

For the sake of simplicity, this will be the target python version.

The `requirements.txt` should therefore be compiled in a python3.11 environment.

```bash
uv pip compile --python-version 3.11 requirements.in --output-file requirements.txt
```
