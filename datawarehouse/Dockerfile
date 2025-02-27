FROM postgres:17.0-bookworm

ENV VIRTUAL_ENV /app/venv
ENV PATH "${VIRTUAL_ENV}/bin:${PATH}"
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

RUN localedef -i fr_FR -c -f UTF-8 -A /usr/share/locale/locale.alias fr_FR.UTF-8

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    postgresql-17-postgis-3 \
    postgresql-plpython3-17 \
    python3-pip \
    python3-venv \
    python3-dev \
    build-essential \
    git-core \
    && apt-get autoremove --purge -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

RUN python3.11 -m venv ${VIRTUAL_ENV}
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

COPY ./docker-entrypoint-initdb.d /docker-entrypoint-initdb.d

COPY processings/requirements processings/requirements
RUN pip install --no-cache-dir -r processings/requirements/requirements.txt

COPY processings processings
RUN pip install --no-cache-dir -e processings
