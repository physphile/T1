import os
from functools import lru_cache

from dotenv import load_dotenv
from pydantic import ConfigDict
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings"""

    load_dotenv(".env")

    DB_DSN: str = os.getenv("DB_DSN", "sqlite+pysqlite:///database.sqlite")
    ROOT_PATH: str = "/" + os.getenv("APP_NAME", "")
    AVAILABLE_CORES: int = os.cpu_count()/2

    CORS_ALLOW_ORIGINS: list[str] = ["*"]
    CORS_ALLOW_CREDENTIALS: bool = True
    CORS_ALLOW_METHODS: list[str] = ["*"]
    CORS_ALLOW_HEADERS: list[str] = ["*"]
    model_config = ConfigDict(case_sensitive=True, env_file=".env", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    return settings
