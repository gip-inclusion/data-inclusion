FROM ghcr.io/mlflow/mlflow:v3.17.2-full@sha256:f922dce73c12026ce7e6b8e3e0740a657db3162910d4368f67dddc86af8b3d10

# extra dependencies of mlflow[auth], installed at build time rather than at startup
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir --no-deps --require-hashes -r requirements.txt \
    && pip check
