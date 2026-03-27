"""Application configuration — loads from environment variables."""

from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Server
    host: str = "0.0.0.0"
    port: int = 8100
    debug: bool = True
    
    # Database
    database_url: str = "sqlite:///db/company_eval.db"
    
    # LLM
    llm_endpoint: str = "http://localhost:1234/v1/chat/completions"
    llm_timeout: int = 120
    llm_temperature: float = 0.0
    
    # Data Sources
    polygon_api_key: str = ""
    finnhub_api_key: str = ""
    polygon_rate_limit: float = 5.0   # Polygon allows higher rates
    finnhub_rate_limit: float = 30.0  # Finnhub free tier: 30 req/sec
    yahoo_rate_limit: float = 1.0     # Yahoo: very conservative
    yahoo_enabled: bool = True        # Fallback — can disable entirely
    
    # Pipeline
    universe: str = "sp500_top100"
    crawler_enabled: bool = False
    crawler_schedule: str = "02:00"
    evaluation_batch_size: int = 10
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

def get_settings() -> Settings:
    return Settings()
