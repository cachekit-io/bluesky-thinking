"""Service configuration (pydantic-settings; secrets are SecretStr)."""

from __future__ import annotations

from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # CACHEKIT_API_KEY: present -> live CachekitIO writes; absent -> dry-run mode.
    cachekit_api_key: SecretStr | None = None
    # CACHEKIT_MASTER_KEY: 64-hex master key for the @cache.secure sentiment cache
    # (AC-6 groundwork). Absent -> the secure cache is disabled, everything else runs.
    cachekit_master_key: SecretStr | None = None

    jetstream_url: str = "wss://jetstream2.us-east.bsky.network/subscribe"
    publish_tick_seconds: float = 15.0
    checkpoint_interval_seconds: float = 120.0
    top_n: int = 50

    @field_validator("cachekit_master_key")
    @classmethod
    def _master_key_is_64_hex(cls, v: SecretStr | None) -> SecretStr | None:
        if v is None:
            return v
        raw = v.get_secret_value()
        if len(raw) != 64 or any(c not in "0123456789abcdefABCDEF" for c in raw):
            raise ValueError("CACHEKIT_MASTER_KEY must be a 64-character hex string")
        return v
