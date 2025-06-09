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
    SENTRY_DSN: str | None = None
    TOKEN_ENABLED: bool = True

    SOLIGUIDE_API_URL: str = "https://api.soliguide.fr"
    SOLIGUIDE_API_TOKEN: str | None = None

    DEFAULT_PAGE_SIZE: int = 500
    MAX_PAGE_SIZE: int = 1000

    DATALAKE_BUCKET_NAME: str | None = None

    AWS_ENDPOINT_URL: str | None = None
    AWS_ACCESS_KEY_ID: str | None = None
    AWS_SECRET_ACCESS_KEY: str | None = None

    @field_validator("DATABASE_URL", mode="before")
    @classmethod
    def valid_postgres_url(cls, value: str) -> str:
        return value.replace("postgres://", "postgresql://")


settings = Settings()
