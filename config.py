"""Application configuration — loads from environment variables."""

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
    
    # Pipeline
    universe: str = "sp500_top100"
    crawler_enabled: bool = False
    crawler_schedule: str = "02:00"
    evaluation_batch_size: int = 10
    
    # Refresh cycle
    refresh_period_days: int = 7
    pause_between_symbols_sec: float = 2.0
    
    @model_validator(mode="after")
    def _resolve_db_path(self):
        """Resolve relative SQLite paths to absolute (safe under PyInstaller)."""
        url = self.database_url
        prefix = "sqlite:///"
        if url.startswith(prefix):
            db_path = url[len(prefix):]
            if not os.path.isabs(db_path):
                abs_path = os.path.join(_PROJECT_ROOT, db_path)
                object.__setattr__(self, "database_url", f"{prefix}{abs_path}")
        return self
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

def get_settings() -> Settings:
    return Settings()
