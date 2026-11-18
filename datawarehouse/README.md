# `datawarehouse`

Custom postgresql image with `plpython3u` enabled.

`plpython3u` is used to embed python transformations within dbt models.

It is generally considered unsafe therefore not available on managed databases.

## compiling the python requirements

The base image is based on debian `trixie` which ships with python3.13.

For the sake of simplicity, this will be the target python version. It is set in the `Makefile` and must be updated when the base image moves to another debian release.

```bash
make -C datawarehouse              # compile requirements.txt
make -C datawarehouse upgrade all  # compile with --upgrade
```
