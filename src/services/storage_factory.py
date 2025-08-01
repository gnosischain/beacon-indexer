from src.config import config
from src.utils.logger import logger

def create_storage():
    """Factory function to create the appropriate storage backend."""
    
    if config.STORAGE_BACKEND.lower() == "parquet":
        from src.services.parquet_storage import ParquetStorage
        logger.info("Using Parquet storage backend")
        return ParquetStorage()
    else:
        from src.services.clickhouse import ClickHouse
        logger.info("Using ClickHouse storage backend")
        return ClickHouse()