FROM postgres:14.9-bookworm

ENV VIRTUAL_ENV /app/venv
ENV PATH "${VIRTUAL_ENV}/bin:${PATH}"
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    postgresql-14-postgis-3 \
    postgresql-plpython3-14 \
    python3-pip \
    python3-venv \
    && apt-get autoremove --purge -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

COPY ./docker-entrypoint-initdb.d /docker-entrypoint-initdb.d

RUN python3.11 -m venv ${VIRTUAL_ENV}

COPY requirements requirements
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements/requirements.txt
