import os
from datetime import timedelta
from pathlib import Path

import dj_database_url

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# Env

ENV = os.environ.get("ENV", "prod")

# API versioning

VERSION_NAME = os.environ.get("VERSION_NAME", None)
VERSION_SHA = os.environ.get("VERSION_SHA", None)

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.environ.get("SECRET_KEY")

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = os.environ.get("DEBUG", False) == "True"

ALLOWED_HOSTS = os.environ.get("ALLOWED_HOSTS") and os.environ.get("ALLOWED_HOSTS", "").split(",") or []


# Application definition

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    # 3rd parties
    "rest_framework",
    "corsheaders",
    "django_filters",
    "drf_spectacular",
    # 1st parties
    "meta",
    "users",
    "inclusion",
]

MIDDLEWARE = list(
    filter(
        lambda s: s is not None,
        [
            "django.middleware.security.SecurityMiddleware",
            "whitenoise.middleware.WhiteNoiseMiddleware" if ENV in ["prod", "staging"] else None,
            "django.contrib.sessions.middleware.SessionMiddleware",
            "corsheaders.middleware.CorsMiddleware",
            "django.middleware.common.CommonMiddleware",
            "django.middleware.csrf.CsrfViewMiddleware",
            "django.contrib.auth.middleware.AuthenticationMiddleware",
            "django.contrib.messages.middleware.MessageMiddleware",
            "django.middleware.clickjacking.XFrameOptionsMiddleware",
        ],
    )
)

ROOT_URLCONF = "urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "wsgi.application"


# Database
# https://docs.djangoproject.com/en/4.0/ref/settings/#databases

if DATABASE_URL := os.environ.get("DATABASE_URL", None):
    # required when the db conn data are passed as an url (scalingo)
    DATABASES = {"default": dj_database_url.config(ssl_require=True)}
else:
    # usual way
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql_psycopg2",
            "NAME": os.environ.get("POSTGRES_DB"),
            "USER": os.environ.get("POSTGRES_USER"),
            "PASSWORD": os.environ.get("POSTGRES_PASSWORD"),
            "HOST": os.environ.get("POSTGRES_HOST"),
            "PORT": int(os.environ.get("POSTGRES_PORT", 5432)),
        }
    }


# Custom User model
# https://docs.djangoproject.com/en/4.0/topics/auth/customizing/#substituting-a-custom-user-model

AUTH_USER_MODEL = "users.User"


# Auth backends
# https://docs.djangoproject.com/en/4.0/topics/auth/customizing/#specifying-authentication-backends

AUTHENTICATION_BACKENDS = [
    "django.contrib.auth.backends.ModelBackend",
]


# Password validation
# https://docs.djangoproject.com/en/4.0/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]


# Internationalization
# https://docs.djangoproject.com/en/4.0/topics/i18n/

LANGUAGE_CODE = "en-us"

TIME_ZONE = "Europe/Paris"

USE_I18N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/4.0/howto/static-files/

STATIC_URL = "/static/"
STATIC_ROOT = Path(os.environ.get("STATIC_ROOT", "var/www/static"))

# Default primary key field type
# https://docs.djangoproject.com/en/4.0/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# CorsHeaders
# Used to access api from third-party domain

CORS_URLS_REGEX = r"^/api/.*$"

# Rest Framework
# https://www.django-rest-framework.org/api-guide/settings/

REST_FRAMEWORK = {
    "DEFAULT_FILTER_BACKENDS": [
        "django_filters.rest_framework.DjangoFilterBackend",
    ],
    "DEFAULT_RENDERER_CLASSES": list(
        filter(
            lambda s: s is not None,
            [
                "rest_framework.renderers.JSONRenderer",
                "rest_framework.renderers.BrowsableAPIRenderer" if ENV in ["dev", "staging"] else None,
            ],
        )
    ),
    "DEFAULT_AUTHENTICATION_CLASSES": [
        "rest_framework.authentication.SessionAuthentication",
        "rest_framework_simplejwt.authentication.JWTAuthentication",
    ],
    "DEFAULT_PERMISSION_CLASSES": [
        "rest_framework.permissions.IsAuthenticated" if ENV == "prod" else "rest_framework.permissions.AllowAny"
    ],
    "DEFAULT_SCHEMA_CLASS": "drf_spectacular.openapi.AutoSchema",
    "DEFAULT_VERSIONING_CLASS": "rest_framework.versioning.NamespaceVersioning",
    "PAGE_SIZE": 100,
}

# Rest Framework simple JWT
# https://django-rest-framework-simplejwt.readthedocs.io/en/latest/settings.html

SIMPLE_JWT = {
    "ACCESS_TOKEN_LIFETIME": timedelta(weeks=24),
}

# API documentation
# https://drf-spectacular.readthedocs.io/en/latest/settings.html

SPECTACULAR_SETTINGS = {
    "TITLE": "data.inclusion API",
    "DESCRIPTION": "API référentiel de l'offre d'insertion",
    "VERSION": VERSION_NAME,
    "SERVE_INCLUDE_SCHEMA": False,
}

# System checks
# https://docs.djangoproject.com/en/4.0/topics/checks/

SILENCED_SYSTEM_CHECKS = [
    "rest_framework.W001",  # pagination_class on a per-view basis
]

# data.gouv.fr
# https://doc.data.gouv.fr/api/intro/

DATAGOUV_API_KEY = os.environ.get("DATAGOUV_API_KEY", None)
DATAGOUV_API_URL = os.environ.get("DATAGOUV_API_URL", None)
DATAGOUV_DI_DATASET_ID = os.environ.get("DATAGOUV_DI_DATASET_ID", None)
DATAGOUV_DI_JSON_STRUCTURE_RESOURCE_ID = os.environ.get("DATAGOUV_DI_JSON_STRUCTURE_RESOURCE_ID", None)
DATAGOUV_DI_CSV_STRUCTURE_RESOURCE_ID = os.environ.get("DATAGOUV_DI_CSV_STRUCTURE_RESOURCE_ID", None)
DATAGOUV_DI_XLSX_STRUCTURE_RESOURCE_ID = os.environ.get("DATAGOUV_DI_XLSX_STRUCTURE_RESOURCE_ID", None)
