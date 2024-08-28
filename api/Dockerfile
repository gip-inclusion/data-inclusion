########
# This image compile the dependencies
########
FROM python:3.11-slim as compile-image

ENV VIRTUAL_ENV /srv/venv
ENV PATH "${VIRTUAL_ENV}/bin:${PATH}"
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

WORKDIR /srv

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
    binutils \
    build-essential \
    libpq-dev \
    git \
    && apt-get autoremove --purge -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv ${VIRTUAL_ENV}

COPY requirements requirements
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements/test-requirements.txt

COPY . .
RUN pip install --no-cache-dir .


########
# This image is the runtime
########
FROM python:3.11-slim as runtime-image

ARG VERSION_SHA
ARG VERSION_NAME
ENV VERSION_SHA $VERSION_SHA
ENV VERSION_NAME $VERSION_NAME

ENV VIRTUAL_ENV /srv/venv
ENV PATH "${VIRTUAL_ENV}/bin:${PATH}"
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

WORKDIR /srv

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
    curl libpq-dev \
    && apt-get autoremove --purge -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd gunicorn
RUN useradd --no-log-init -g gunicorn gunicorn

# Copy venv with compiled dependencies
COPY --chown=gunicorn:gunicorn --from=compile-image /srv/venv /srv/venv

COPY --chown=gunicorn:gunicorn ["execute_and_notify.sh", "docker-entrypoint.sh", "pyproject.toml", "alembic.ini", "/srv/"]
COPY --chown=gunicorn:gunicorn src /srv/src
COPY --chown=gunicorn:gunicorn tests /srv/tests


RUN chmod +x docker-entrypoint.sh
RUN chmod +x execute_and_notify.sh

USER gunicorn
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 CMD curl --fail http://localhost:8000/api/openapi.json

ENTRYPOINT ["/srv/docker-entrypoint.sh"]
