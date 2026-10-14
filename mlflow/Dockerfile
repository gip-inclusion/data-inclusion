FROM ghcr.io/mlflow/mlflow:v3.12.0-full@sha256:364cdfa503f1a54a000e7f86f3910f9a838ea939d225b6394710010e4444f8a2

# extra dependencies of mlflow[auth], installed at build time rather than at startup
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir --no-deps --require-hashes -r requirements.txt \
    && pip check
