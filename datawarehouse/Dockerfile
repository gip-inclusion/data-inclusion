FROM postgres:17.4-bookworm

ENV VIRTUAL_ENV=/app/venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    postgresql-17-postgis-3 \
    postgresql-plpython3-17 \
    python3-pip \
    python3-venv \
    python3-dev \
    build-essential \
    git-core \
    curl \
    wget \
    ca-certificates \
    && wget -O /usr/local/share/ca-certificates/mozilla-cacert.crt https://curl.se/ca/cacert.pem \
    && wget -O /usr/local/share/ca-certificates/google-roots.crt https://pki.goog/roots.pem \
    && update-ca-certificates --fresh \
    && apt-get autoremove --purge -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

RUN python3.11 -m venv ${VIRTUAL_ENV}
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

COPY ./docker-entrypoint-initdb.d /docker-entrypoint-initdb.d

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY deduplicate-model.bin /app/deduplicate-model.bin
ENV DEDUPLICATE_MODEL_PATH=/app/deduplicate-model.bin

RUN localedef -i fr_FR -c -f UTF-8 -A /usr/share/locale/locale.alias fr_FR.UTF-8
