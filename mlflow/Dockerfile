FROM ghcr.io/mlflow/mlflow:v3.18.0-full@sha256:a8459ca08b1e8a0dc287b4e226d4f2338f56052d7e2d333a4aa5891be0670919

# extra dependencies of mlflow[auth], installed at build time rather than at startup
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir --no-deps --require-hashes -r requirements.txt \
    && pip check
