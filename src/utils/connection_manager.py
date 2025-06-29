"""
Connection manager for thread-safe database and API connections.
Provides connection pooling and lifecycle management.
"""
import threading
import queue
import time
from typing import Dict, Optional, Any
from contextlib import contextmanager

from src.services.clickhouse_service import ClickHouseService
from src.services.beacon_api_service import BeaconAPIService
from src.utils.logger import logger


class ThreadLocalConnectionManager:
    """
    Manages thread-local connections for workers.
    Each thread gets its own set of connections.
    """
    
    def __init__(self):
        self.local = threading.local()
        self.config = {}
        self.lock = threading.Lock()
        
    def configure(self, beacon_url: str, clickhouse_config: Dict[str, Any]):
        """Configure connection parameters."""
        with self.lock:
            self.config['beacon_url'] = beacon_url
            self.config['clickhouse'] = clickhouse_config.copy()
            
    def get_clickhouse(self) -> ClickHouseService:
        """Get thread-local ClickHouse connection."""
        if not hasattr(self.local, 'clickhouse'):
            self.local.clickhouse = ClickHouseService(**self.config['clickhouse'])
        return self.local.clickhouse
        
    def get_beacon_api(self) -> BeaconAPIService:
        """Get thread-local Beacon API connection."""
        if not hasattr(self.local, 'beacon_api'):
            self.local.beacon_api = BeaconAPIService(self.config['beacon_url'])
        return self.local.beacon_api
        
    def cleanup_thread(self):
        """Clean up connections for current thread."""
        if hasattr(self.local, 'beacon_api'):
            # Beacon API cleanup handled elsewhere (async)
            del self.local.beacon_api
            
        if hasattr(self.local, 'clickhouse'):
            # ClickHouse connections are cleaned up automatically
            del self.local.clickhouse


class PooledConnectionManager:
    """
    Manages a pool of reusable connections.
    Better for scenarios with many short-lived operations.
    """
    
    def __init__(self, pool_size: int = 10):
        self.pool_size = pool_size
        self.clickhouse_pool = queue.Queue(maxsize=pool_size)
        self.beacon_pool = queue.Queue(maxsize=pool_size)
        self.config = {}
        self.initialized = False
        self.lock = threading.Lock()
        
    def initialize(self, beacon_url: str, clickhouse_config: Dict[str, Any]):
        """Initialize the connection pools."""
        with self.lock:
            if self.initialized:
                return
                
            self.config['beacon_url'] = beacon_url
            self.config['clickhouse'] = clickhouse_config.copy()
            
            # Pre-create ClickHouse connections
            for _ in range(self.pool_size):
                conn = ClickHouseService(**self.config['clickhouse'])
                self.clickhouse_pool.put(conn)
                
            # Beacon API connections created on demand (async)
            for _ in range(self.pool_size):
                self.beacon_pool.put(None)  # Placeholder
                
            self.initialized = True
            logger.info(f"Initialized connection pool with size {self.pool_size}")
            
    @contextmanager
    def get_clickhouse_connection(self, timeout: float = 5.0):
        """
        Get a ClickHouse connection from the pool.
        Use as context manager to ensure connection is returned.
        """
        conn = None
        try:
            conn = self.clickhouse_pool.get(timeout=timeout)
            yield conn
        finally:
            if conn:
                self.clickhouse_pool.put(conn)
                
    def get_beacon_url(self) -> str:
        """Get the configured beacon URL for creating new connections."""
        return self.config.get('beacon_url', '')
        
    def shutdown(self):
        """Shutdown the connection pool."""
        with self.lock:
            if not self.initialized:
                return
                
            # Drain ClickHouse pool
            while not self.clickhouse_pool.empty():
                try:
                    self.clickhouse_pool.get_nowait()
                except queue.Empty:
                    break
                    
            # Drain beacon pool
            while not self.beacon_pool.empty():
                try:
                    self.beacon_pool.get_nowait()
                except queue.Empty:
                    break
                    
            self.initialized = False
            logger.info("Connection pool shut down")


# Global instances
thread_local_manager = ThreadLocalConnectionManager()
pooled_manager = PooledConnectionManager()


def get_connection_manager(pooled: bool = False):
    """
    Get the appropriate connection manager.
    
    Args:
        pooled: If True, return pooled manager. Otherwise, thread-local.
    """
    return pooled_manager if pooled else thread_local_manager