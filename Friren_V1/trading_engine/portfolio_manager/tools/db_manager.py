"""
Trading Database Manager for Portfolio Manager

Lightweight SQL tool that reuses Django models for schema awareness
while providing direct database access for multiprocess trading system.
"""

import os
import sys
import psycopg2
import psycopg2.extras
import logging
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from decimal import Decimal
from contextlib import contextmanager
from dataclasses import dataclass
import json

# Ensure Django is properly configured before importing models
def setup_django():
    """Setup Django configuration once"""
    try:
        import django
        from django.conf import settings

        if not settings.configured:
            os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
            django.setup()
        return True
    except Exception as e:
        logging.error(f"Django setup failed: {e}")
        return False

# Initialize Django
DJANGO_AVAILABLE = setup_django()

# Import Django models for schema awareness
if DJANGO_AVAILABLE:
    try:
        from infrastructure.database.models import TransactionHistory, CurrentHoldings, MLFeatures
    except ImportError as e:
        logging.warning(f"Could not import Django models: {e}")
        DJANGO_AVAILABLE = False

@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str = "localhost"
    port: int = 5432
    database: str = "friren_trading_db"
    user: str = "friren_user"
    password: str = "secure_password_here"

    @classmethod
    def from_env(cls):
        """Load configuration from environment variables"""
        return cls(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            database=os.getenv("DB_NAME", "friren_trading_db"),
            user=os.getenv("DB_USER", "friren_user"),
            password=os.getenv("DB_PASSWORD", "secure_password_here")
        )

class TradingDBManager:
    """
    Lightweight database manager for the trading system

    Provides direct SQL access while being aware of Django model schemas.
    Optimized for multiprocess trading system with minimal dependencies.
    """

    def __init__(self, config: Optional[DatabaseConfig] = None):
        self.config = config or DatabaseConfig.from_env()
        self.logger = logging.getLogger(__name__)
        self._connection = None

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            yield conn
        except psycopg2.Error as e:
            self.logger.error(f"Database connection error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()

    def health_check(self) -> Dict[str, Any]:
        """Check database connectivity and basic metrics"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Test basic connectivity
                    cursor.execute("SELECT 1")

                    # Get table counts
                    tables = {}

                    # Check if tables exist before querying them
                    for table_name in ['transaction_history', 'current_holdings', 'ml_features', 'watchlist']:
                        try:
                            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                            tables[table_name] = cursor.fetchone()['count']
                        except psycopg2.Error:
                            tables[table_name] = "Table not found"

                    return {
                        "status": "healthy",
                        "tables": tables,
                        "django_available": DJANGO_AVAILABLE,
                        "timestamp": datetime.now().isoformat()
                    }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "django_available": DJANGO_AVAILABLE,
                "timestamp": datetime.now().isoformat()
            }

    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict]:
        """Execute a SELECT query and return results"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()

    def execute_update(self, query: str, params: Optional[tuple] = None) -> int:
        """Execute an INSERT/UPDATE/DELETE query and return affected rows"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                conn.commit()
                return cursor.rowcount

    # Portfolio Management Methods
    def get_current_holdings(self) -> List[Dict]:
        """Get all current portfolio holdings"""
        try:
            query = """
            SELECT symbol, quantity, avg_cost, current_price,
                   last_updated, strategy_used
            FROM current_holdings
            WHERE quantity > 0
            ORDER BY symbol
            """
            return self.execute_query(query)
        except psycopg2.Error as e:
            self.logger.error(f"Error getting holdings: {e}")
            return []

    def get_holding(self, symbol: str) -> Optional[Dict]:
        """Get holding information for a specific symbol"""
        try:
            query = """
            SELECT symbol, quantity, avg_cost, current_price,
                   last_updated, strategy_used
            FROM current_holdings
            WHERE symbol = %s
            """
            results = self.execute_query(query, (symbol,))
            return results[0] if results else None
        except psycopg2.Error as e:
            self.logger.error(f"Error getting holding for {symbol}: {e}")
            return None

    def update_holding(self, symbol: str, quantity: float, avg_cost: float,
                      current_price: float, strategy_used: str = "unknown") -> bool:
        """Update or insert holding information"""
        try:
            query = """
            INSERT INTO current_holdings (symbol, quantity, avg_cost, current_price, strategy_used, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol) DO UPDATE SET
                quantity = EXCLUDED.quantity,
                avg_cost = EXCLUDED.avg_cost,
                current_price = EXCLUDED.current_price,
                strategy_used = EXCLUDED.strategy_used,
                last_updated = EXCLUDED.last_updated
            """
            params = (symbol, quantity, avg_cost, current_price, strategy_used, datetime.now())
            rows_affected = self.execute_update(query, params)
            return rows_affected > 0
        except psycopg2.Error as e:
            self.logger.error(f"Error updating holding for {symbol}: {e}")
            return False

    def record_transaction(self, symbol: str, action: str, quantity: float,
                          price: float, strategy_used: str = "unknown",
                          metadata: Optional[Dict] = None) -> bool:
        """Record a trading transaction"""
        try:
            # Create table if it doesn't exist
            self._ensure_transaction_table()

            query = """
            INSERT INTO transaction_history (symbol, action, quantity, price, strategy_used, metadata, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            params = (symbol, action, quantity, price, strategy_used,
                     json.dumps(metadata or {}), datetime.now())
            rows_affected = self.execute_update(query, params)
            return rows_affected > 0
        except psycopg2.Error as e:
            self.logger.error(f"Error recording transaction: {e}")
            return False

    def get_transaction_history(self, symbol: Optional[str] = None,
                               limit: int = 100) -> List[Dict]:
        """Get transaction history, optionally filtered by symbol"""
        try:
            if symbol:
                query = """
                SELECT symbol, action, quantity, price, strategy_used, metadata, timestamp
                FROM transaction_history
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT %s
                """
                params = (symbol, limit)
            else:
                query = """
                SELECT symbol, action, quantity, price, strategy_used, metadata, timestamp
                FROM transaction_history
                ORDER BY timestamp DESC
                LIMIT %s
                """
                params = (limit,)

            return self.execute_query(query, params)
        except psycopg2.Error as e:
            self.logger.error(f"Error getting transaction history: {e}")
            return []

    # Watchlist Management Methods
    def add_watchlist_symbol(self, symbol: str, discovery_reason: str,
                           sentiment_score: float, confidence: float,
                           priority: int = 5, key_articles: Optional[List[str]] = None) -> bool:
        """Add a symbol to the watchlist"""
        try:
            # Create watchlist table if it doesn't exist
            self._ensure_watchlist_table()

            query = """
            INSERT INTO watchlist (symbol, discovery_reason, sentiment_score, confidence,
                                 priority, key_articles, added_date, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol) DO UPDATE SET
                discovery_reason = EXCLUDED.discovery_reason,
                sentiment_score = EXCLUDED.sentiment_score,
                confidence = EXCLUDED.confidence,
                priority = EXCLUDED.priority,
                key_articles = EXCLUDED.key_articles,
                updated_date = %s,
                is_active = TRUE
            """
            now = datetime.now()
            params = (symbol, discovery_reason, sentiment_score, confidence,
                     priority, json.dumps(key_articles or []), now, True, now)
            rows_affected = self.execute_update(query, params)
            return rows_affected > 0
        except psycopg2.Error as e:
            self.logger.error(f"Error adding watchlist symbol {symbol}: {e}")
            return False

    def get_watchlist(self, active_only: bool = True) -> List[Dict]:
        """Get all watchlist symbols"""
        try:
            query = """
            SELECT symbol, discovery_reason, sentiment_score, confidence,
                   priority, key_articles, added_date, updated_date
            FROM watchlist
            WHERE is_active = %s OR %s = FALSE
            ORDER BY priority DESC, sentiment_score DESC
            """
            return self.execute_query(query, (True, active_only))
        except psycopg2.Error as e:
            self.logger.error(f"Error getting watchlist: {e}")
            return []

    def remove_from_watchlist(self, symbol: str) -> bool:
        """Remove symbol from watchlist (mark as inactive)"""
        try:
            query = """
            UPDATE watchlist
            SET is_active = FALSE, updated_date = %s
            WHERE symbol = %s
            """
            rows_affected = self.execute_update(query, (datetime.now(), symbol))
            return rows_affected > 0
        except psycopg2.Error as e:
            self.logger.error(f"Error removing watchlist symbol {symbol}: {e}")
            return False

    def get_watchlist_symbol_info(self, symbol: str) -> Optional[Dict]:
        """Get detailed information about a watchlist symbol"""
        try:
            query = """
            SELECT symbol, discovery_reason, sentiment_score, confidence,
                   priority, key_articles, added_date, updated_date, is_active
            FROM watchlist
            WHERE symbol = %s
            """
            results = self.execute_query(query, (symbol,))
            return results[0] if results else None
        except psycopg2.Error as e:
            self.logger.error(f"Error getting watchlist info for {symbol}: {e}")
            return None

    # Helper methods for table creation
    def _ensure_transaction_table(self):
        """Ensure transaction_history table exists"""
        try:
            query = """
            CREATE TABLE IF NOT EXISTS transaction_history (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                action VARCHAR(10) NOT NULL,
                quantity DECIMAL(12, 4) NOT NULL,
                price DECIMAL(12, 4) NOT NULL,
                strategy_used VARCHAR(50) DEFAULT 'unknown',
                metadata JSONB DEFAULT '{}',
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            self.execute_update(query)

            # Create index if not exists
            index_query = """
            CREATE INDEX IF NOT EXISTS idx_transaction_symbol_timestamp
            ON transaction_history (symbol, timestamp DESC)
            """
            self.execute_update(index_query)
        except psycopg2.Error as e:
            self.logger.error(f"Error creating transaction table: {e}")

    def _ensure_watchlist_table(self):
        """Ensure watchlist table exists"""
        try:
            query = """
            CREATE TABLE IF NOT EXISTS watchlist (
                symbol VARCHAR(10) PRIMARY KEY,
                discovery_reason TEXT NOT NULL,
                sentiment_score DECIMAL(5, 4) NOT NULL,
                confidence DECIMAL(5, 4) NOT NULL,
                priority INTEGER DEFAULT 5,
                key_articles JSONB DEFAULT '[]',
                added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            )
            """
            self.execute_update(query)
        except psycopg2.Error as e:
            self.logger.error(f"Error creating watchlist table: {e}")

# Factory function for easy instantiation
def create_trading_db_manager(config: Optional[DatabaseConfig] = None) -> TradingDBManager:
    """Create a TradingDBManager instance with optional configuration"""
    return TradingDBManager(config)
