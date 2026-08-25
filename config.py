"""Configuration for CNPJ data pipeline."""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    """Pipeline configuration with sensible defaults."""

    database_url: str
    batch_size: int = 500000
    temp_dir: str = "./temp"
    download_workers: int = 4
    retry_attempts: int = 3
    retry_delay: int = 5
    connect_timeout: int = 30
    read_timeout: int = 300
    keep_files: bool = False
    neon_slim: bool = False  # opt-in: 1 = modo enxuto (Neon); nao quebra carga local completa
    load_replace: bool = False  # opt-in: 1 = TRUNCATE por tabela (snapshot mensal)
    base_url: str = "https://arquivos.receitafederal.gov.br/public.php/webdav"
    share_token: str = "YggdBLfdninEJX9"

    @classmethod
    def from_env(cls) -> "Config":
        """Create config from environment variables."""
        return cls(
            database_url=os.getenv("DATABASE_URL", ""),
            batch_size=int(os.getenv("BATCH_SIZE", "500000")),
            temp_dir=os.getenv("TEMP_DIR", "./temp"),
            download_workers=int(os.getenv("DOWNLOAD_WORKERS", "4")),
            retry_attempts=int(os.getenv("RETRY_ATTEMPTS", "3")),
            retry_delay=int(os.getenv("RETRY_DELAY", "5")),
            connect_timeout=int(os.getenv("CONNECT_TIMEOUT", "30")),
            read_timeout=int(os.getenv("READ_TIMEOUT", "300")),
            keep_files=os.getenv("KEEP_DOWNLOADED_FILES", "false").lower() == "true",
            neon_slim=os.getenv("CNPJ_NEON_SLIM", "0") == "1",
            load_replace=os.getenv("CNPJ_LOAD_REPLACE", "0") == "1",
        )


config = Config.from_env()
