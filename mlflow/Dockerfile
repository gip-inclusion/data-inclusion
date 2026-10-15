FROM ghcr.io/mlflow/mlflow:v3.17.1-full@sha256:190306b1739b9646d6d39333594334b69ba14ab843db927712c790b50892f262

# extra dependencies of mlflow[auth], installed at build time rather than at startup
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir --no-deps --require-hashes -r requirements.txt \
    && pip check
