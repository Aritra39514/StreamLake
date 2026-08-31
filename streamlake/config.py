from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    warehouse_path: str = str(Path("warehouse").resolve())
    catalog_db_uri: str = "sqlite:///streamlake_catalog.db"
    state_db_path: str = "streamlake_state.db"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    batch_size: int = 100
    batch_timeout_seconds: float = 5.0
    metrics_port: int = 8001

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
