from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    ALLOWED_HOSTS: list[str] = ["api.data.inclusion.gouv.fr"]
    BASE_URL: str = "https://api.data.inclusion.gouv.fr"
    CORS_ALLOWED_ORIGINS: list[str] = ["*"]
    DATABASE_URL: str
    DEBUG: bool = False
    ENV: str = "prod"
    SECRET_KEY: str
    # On 2025-04-12 this key has been rotated. We keep compatibility
    # with existing tokens until full renewal.
    OLD_SECRET_KEY: str | None = None
    OLD_USER_SUBS: list[str] = []  # use JSON list in the env var
    SENTRY_DSN: str | None = None

    SOLIGUIDE_API_URL: str = "https://api.soliguide.fr"
    SOLIGUIDE_API_TOKEN: str | None = None

    DEFAULT_PAGE_SIZE: int = 5_000
    MAX_PAGE_SIZE: int = 10_000

    DATALAKE_BUCKET_NAME: str | None = None

    AWS_ENDPOINT_URL: str | None = None
    AWS_ACCESS_KEY_ID: str | None = None
    AWS_SECRET_ACCESS_KEY: str | None = None

    @field_validator("DATABASE_URL", mode="before")
    @classmethod
    def valid_postgres_url(cls, value: str) -> str:
        return value.replace("postgres://", "postgresql://")


settings = Settings()

assert settings.SECRET_KEY != settings.OLD_SECRET_KEY
