"""Application configuration — loads from environment variables."""

import json
import os
import sys
from pydantic import model_validator
from pydantic_settings import BaseSettings

# Compute absolute project root — safe under PyInstaller
if getattr(sys, "frozen", False):
    _PROJECT_ROOT = os.path.dirname(sys.executable)
else:
    _PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

DB_DIR = os.path.join(_PROJECT_ROOT, "db")
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "company_eval.db")
_DEFAULT_DB_URL = f"sqlite:///{DB_PATH}"
_SQLITE_URL_PREFIX = "sqlite:///"


def sqlite_url_to_path(database_url: str) -> str:
    """Convert a SQLite URL into a filesystem path."""
    if not database_url.startswith(_SQLITE_URL_PREFIX):
        raise ValueError(f"Unsupported database URL: {database_url}")

    if database_url.startswith("sqlite:////"):
        unc_target = database_url[len("sqlite:////"):]
        if unc_target and not os.path.splitdrive(unc_target)[0]:
            return "\\\\" + unc_target.replace("/", "\\")

    raw_path = database_url[len(_SQLITE_URL_PREFIX):]
    return raw_path.replace("/", os.sep)


def sqlite_path_to_url(database_path: str) -> str:
    """Convert a filesystem path into a SQLite URL."""
    normalized_path = os.path.normpath(database_path)
    if normalized_path.startswith("\\\\"):
        return "sqlite:////" + normalized_path.lstrip("\\").replace("\\", "/")
    return f"{_SQLITE_URL_PREFIX}{normalized_path}"


class Settings(BaseSettings):
    # Server
    host: str = "0.0.0.0"
    port: int = 8100
    debug: bool = True
    
    # Database
    database_url: str = _DEFAULT_DB_URL
    
    # LLM
    llm_endpoint: str = "http://localhost:1234/v1/chat/completions"
    llm_model: str = ""  # empty = auto-detect from LM Studio
    llm_timeout: int = 120
    llm_temperature: float = 0.0
    
    # Data Sources
    polygon_api_key: str = ""
    finnhub_api_key: str = ""
    polygon_rate_limit: float = 100.0  # Polygon Starter: unlimited, 100ms courtesy delay
    finnhub_rate_limit: float = 30.0  # Finnhub free tier: 30 req/sec
    yahoo_rate_limit: float = 1.0     # Yahoo: very conservative
    yahoo_enabled: bool = True        # Fallback — can disable entirely

    # FMP (Financial Modeling Prep) — cross-validator + Polygon fallback
    fmp_api_key: str = ""
    fmp_enabled: bool = False         # Enable FMP (cross-validation + statement fallback)
    fmp_rate_limit_per_min: int = 300 # Paid tier: 300 req/min
    fmp_base_url: str = "https://financialmodelingprep.com/stable"
    enable_bulk_cache: bool = True    # Wrap FMPClient with CachedFMPClient when bulk DB exists
    bulk_cache_path: str = ""         # Override bulk cache DB path (empty = default alongside main DB)
    bulk_cache_stale_hours: int = 24  # Crawler-triggered refresh threshold
    bulk_auto_refresh: bool = True    # Auto-refresh stale cache at cycle start (Phase 2c)

    # Data source routing — per-call-site overrides for Polygon→FMP migration.
    # JSON string mapping call-site keys to "polygon", "fmp", or "shadow".
    # Unspecified call sites fall back to "default" key, then to "polygon".
    # Example: '{"default":"polygon","entry_point.get_snapshot":"fmp"}'
    data_source_overrides: str = "{}"

    # Pipeline
    universe: str = "sp500_top100"
    crawler_enabled: bool = False
    crawler_schedule: str = "02:00"
    evaluation_batch_size: int = 10
    
    # Refresh cycle
    refresh_period_days: int = 7
    pause_between_symbols_sec: float = 2.0
    rankings_update_interval: int = 50
    
    @model_validator(mode="after")
    def _resolve_db_path(self):
        """Resolve relative SQLite paths to absolute (safe under PyInstaller)."""
        url = self.database_url
        if url.startswith(_SQLITE_URL_PREFIX):
            db_path = sqlite_url_to_path(url)
            if not os.path.isabs(db_path):
                abs_path = os.path.join(_PROJECT_ROOT, db_path)
                object.__setattr__(self, "database_url", sqlite_path_to_url(abs_path))
        return self

    @property
    def database_path(self) -> str:
        return sqlite_url_to_path(self.database_url)

    def get_data_source(self, call_site_key: str) -> str:
        """Return "polygon", "fmp", or "shadow" for a given call site.

        Resolution order:
          1. Exact match in data_source_overrides
          2. "default" key in data_source_overrides
          3. "polygon" (hardcoded ultimate fallback)
        """
        try:
            overrides = json.loads(self.data_source_overrides)
        except (json.JSONDecodeError, TypeError):
            overrides = {}
        value = overrides.get(call_site_key, overrides.get("default", "polygon"))
        if value not in ("polygon", "fmp", "shadow"):
            return "polygon"
        return value
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

def get_settings() -> Settings:
    return Settings()


# ── Known call-site keys (documentation / iteration) ──────────
DATA_SOURCE_CALL_SITES = [
    "company_data_service.get_financials",
    "company_data_service.get_company_details",
    "company_data_service.get_price_history",
    "entry_point.get_raw_bars",
    "entry_point.get_rsi",
    "entry_point.get_sma",
    "entry_point.get_macd",
    "entry_point.get_snapshot",
    "chart_service.get_raw_bars",
    "routes_quote.get_snapshot",
    "routes_admin.get_company_details",
    "on_demand.get_company_details",
    "on_demand.get_snapshot",
    "universe_builder.get_tickers",
]
