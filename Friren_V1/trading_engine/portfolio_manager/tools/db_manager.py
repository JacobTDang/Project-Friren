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
        # Check for RDS credentials first (primary database configuration)
        rds_endpoint = os.getenv("RDS_ENDPOINT", "")
        rds_username = os.getenv("RDS_USERNAME", "")
        rds_password = os.getenv("RDS_PASSWORD", "")
        rds_dbname = os.getenv("RDS_DBNAME", "")

        # If RDS credentials are available, use them directly
        if rds_endpoint and rds_username and rds_password:
            return cls(
                host=rds_endpoint,
                port=int(os.getenv("RDS_PORT", "5432")),
                database=rds_dbname,
                user=rds_username,
                password=rds_password
            )

        # Fall back to standard DB_ environment variables
        db_host = os.getenv("DB_HOST", "")
        db_user = os.getenv("DB_USER", "")
        db_password = os.getenv("DB_PASSWORD", "")

        if db_host and db_user and db_password:
            return cls(
                host=db_host,
                port=int(os.getenv("DB_PORT", "5432")),
                database=os.getenv("DB_NAME", "friren_trading_db"),
                user=db_user,
                password=db_password
            )

        # Final fallback - return configuration that will gracefully fail
        return cls(
            host="localhost",
            port=5432,
            database="trading_db_not_configured",
            user="fallback_user",
            password="fallback_password"
        )

class TradingDBManager:
    """
    Lightweight database manager for the trading system

    Provides direct SQL access while being aware of Django model schemas.
    Optimized for multiprocess trading system with minimal dependencies.
    """

    def __init__(self, config: Optional[Union[DatabaseConfig, str]] = None):
        """Initialize the database manager"""
        # Handle both DatabaseConfig objects and string identifiers for backwards compatibility
        if isinstance(config, str):
            # If a string is passed, it's likely a component identifier - use default config
            self.component_id = config
            self.config = DatabaseConfig.from_env()
        elif isinstance(config, DatabaseConfig):
            self.component_id = "trading_db"
            self.config = config
        else:
            self.component_id = "trading_db"
            self.config = DatabaseConfig.from_env()
            
        self.logger = logging.getLogger(__name__)

        # Only initialize tables if database is actually configured
        try:
            self._ensure_current_holdings_table()
            self._ensure_transaction_table()
            self._ensure_watchlist_table()
        except Exception as e:
            self.logger.warning(f"Database initialization failed: {e}. Running in fallback mode.")
            # Continue operation without database - Redis will handle the data

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
                    for table_name in ['transaction_history', 'current_holdings', 'ml_features', 'trading_watchlist']:
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
    def get_current_holdings(self) -> List[Dict[str, Any]]:
        """Get current trading positions/holdings"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT symbol, net_quantity, avg_cost_basis, total_invested,
                               first_purchase_date, last_transaction_date, is_active
                        FROM current_holdings
                        WHERE is_active = true AND net_quantity > 0
                        ORDER BY symbol
                    """)

                    holdings = []
                    for row in cursor.fetchall():
                        holdings.append({
                            'symbol': row['symbol'],
                            'quantity': row['net_quantity'],  # Map net_quantity to quantity for code compatibility
                            'avg_cost': row['avg_cost_basis'],
                            'market_value': 0.0,   # Will be calculated
                            'unrealized_pnl': 0.0, # Will be calculated
                            'total_invested': float(row['total_invested']) if row['total_invested'] else 0.0,
                            'first_purchase_date': row['first_purchase_date'],
                            'last_transaction_date': row['last_transaction_date']
                        })

                    return holdings

        except Exception as e:
            self.logger.error(f"Database connection error: {e}")
            self.logger.error(f"Error getting holdings: {e}")
            return []

    def get_holdings(self, active_only: bool = True, **kwargs) -> List[Dict]:
        """Alias for get_current_holdings for compatibility"""
        try:
            if active_only:
                return self.get_current_holdings()
            else:
                # Return all holdings including zero positions if active_only=False
                query = """
                SELECT symbol, net_quantity, avg_cost_basis, total_invested,
                       last_transaction_date, is_active
                FROM current_holdings
                ORDER BY symbol
                """
                return self.execute_query(query)
        except Exception as e:
            self.logger.error(f"Error getting holdings: {e}")
            return []

    def get_portfolio_summary(self) -> Dict[str, Any]:
        """Get portfolio summary statistics"""
        try:
            query = """
            SELECT COUNT(*) as total_positions,
                   COALESCE(SUM(net_quantity * avg_cost_basis), 0) as total_invested
            FROM current_holdings
            WHERE net_quantity > 0
            """
            result = self.execute_query(query)

            if result:
                summary = result[0]
                total_invested = summary.get('total_invested')
                # Handle None values safely
                if total_invested is None:
                    total_invested = 0.0
                else:
                    total_invested = float(total_invested)

                return {
                    'total_positions': summary.get('total_positions', 0),
                    'total_invested': total_invested,
                }
            else:
                return {
                    'total_positions': 0,
                    'total_invested': 0.0,
                }

        except psycopg2.Error as e:
            self.logger.error(f"Error getting portfolio summary: {e}")
            return {
                'total_positions': 0,
                'total_invested': 0.0,
            }

    def get_holding(self, symbol: str) -> Optional[Dict]:
        """Get holding information for a specific symbol"""
        try:
            query = """
            SELECT symbol, net_quantity, avg_cost_basis, total_invested,
                   last_transaction_date, is_active
            FROM current_holdings
            WHERE symbol = %s
            """
            results = self.execute_query(query, (symbol,))
            return results[0] if results else None
        except psycopg2.Error as e:
            self.logger.error(f"Error getting holding for {symbol}: {e}")
            return None

    def update_holding(self, symbol: str, net_quantity: float, avg_cost_basis: float,
                      strategy_used: str = "unknown", realized_pnl: float = 0.0) -> bool:
        """Update or insert holding information"""
        try:
            # Ensure table exists with correct structure
            self._ensure_current_holdings_table()

            # Calculate total_invested
            total_invested = net_quantity * avg_cost_basis

            # Get current timestamp
            now = datetime.now()

            query = """
            INSERT INTO current_holdings (symbol, net_quantity, avg_cost_basis, total_invested,
                                        realized_pnl, first_purchase_date, last_transaction_date,
                                        number_of_transactions, is_active, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol) DO UPDATE SET
                net_quantity = EXCLUDED.net_quantity,
                avg_cost_basis = EXCLUDED.avg_cost_basis,
                total_invested = EXCLUDED.total_invested,
                realized_pnl = EXCLUDED.realized_pnl,
                last_transaction_date = EXCLUDED.last_transaction_date,
                number_of_transactions = current_holdings.number_of_transactions + 1,
                is_active = EXCLUDED.is_active,
                updated_at = EXCLUDED.updated_at
            """

            # Set is_active based on quantity
            is_active = net_quantity != 0

            # For new positions, use current time for first_purchase_date
            # For existing positions, this will be ignored due to ON CONFLICT
            first_purchase_date = now

            self.execute_update(query, (
                symbol, net_quantity, avg_cost_basis, total_invested, realized_pnl,
                first_purchase_date, now, 1, is_active, now, now
            ))

            self.logger.info(f"Successfully updated holding for {symbol}: {net_quantity} shares @ ${avg_cost_basis:.2f}")
            return True

        except Exception as e:
            self.logger.error(f"Error updating holding for {symbol}: {e}")
            return False

    def _ensure_current_holdings_table(self):
        """Ensure current_holdings table exists with correct structure"""
        try:
            query = """
            CREATE TABLE IF NOT EXISTS current_holdings (
                holdings_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                symbol VARCHAR(20) UNIQUE NOT NULL,
                net_quantity DECIMAL(15, 6) NOT NULL,
                avg_cost_basis DECIMAL(15, 6) NOT NULL,
                total_invested DECIMAL(15, 2) NOT NULL,
                realized_pnl DECIMAL(15, 2) DEFAULT 0,
                first_purchase_date TIMESTAMP NOT NULL,
                last_transaction_date TIMESTAMP NOT NULL,
                number_of_transactions INTEGER DEFAULT 0,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            self.execute_update(query)

            # Fix UUID default if table exists but doesn't have the default
            try:
                self.execute_update("""
                    ALTER TABLE current_holdings
                    ALTER COLUMN holdings_id SET DEFAULT gen_random_uuid()
                """)
                self.logger.info("Fixed UUID default for current_holdings table")
            except psycopg2.Error as e:
                # Ignore error if default already exists
                self.logger.debug(f"UUID default already exists or error: {e}")

            # Create indexes if they don't exist
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_current_holdings_symbol ON current_holdings (symbol)",
                "CREATE INDEX IF NOT EXISTS idx_current_holdings_active ON current_holdings (is_active)",
                "CREATE INDEX IF NOT EXISTS idx_current_holdings_last_transaction ON current_holdings (last_transaction_date)"
            ]

            for index_query in indexes:
                self.execute_update(index_query)

        except psycopg2.Error as e:
            self.logger.error(f"Error creating current_holdings table: {e}")

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
    def add_watchlist_symbol(self, symbol: str, sentiment_score=None, notes=None, **kwargs):
        """Add a symbol to the trading watchlist"""
        try:
            query = """
            INSERT INTO trading_watchlist (symbol, sentiment_score, notes)
            VALUES (%s, %s, %s)
            ON CONFLICT(symbol) DO UPDATE SET
                sentiment_score = EXCLUDED.sentiment_score,
                notes = EXCLUDED.notes
            """
            params = (symbol, sentiment_score, notes)
            self.execute_query(query, params)
        except Exception as e:
            self.logger.error(f"Failed to add symbol to watchlist: {e}")

    def get_watchlist(self, active_only: bool = True) -> List[Dict[str, Any]]:
        try:
            query = """
            SELECT symbol, sentiment_score, notes, status, priority,
                   target_entry_price, stop_loss_price, take_profit_price,
                   max_position_size_pct, sector, market_cap, added_date,
                   last_analyzed, is_active
            FROM trading_watchlist
            WHERE is_active = TRUE
            ORDER BY priority DESC, symbol
            """
            result = self.execute_query(query)
            if not result or not isinstance(result, list) or len(result) == 0:
                return []
            # Defensive: ensure all elements are dicts, else convert
            columns = ['symbol', 'sentiment_score', 'notes', 'status', 'priority',
                      'target_entry_price', 'stop_loss_price', 'take_profit_price',
                      'max_position_size_pct', 'sector', 'market_cap', 'added_date',
                      'last_analyzed', 'is_active']
            dict_result = []
            for row in result:
                if isinstance(row, dict):
                    dict_result.append(row)
                else:
                    dict_result.append(dict(zip(columns, row)))
            return dict_result
        except Exception as e:
            self.logger.error(f"Failed to get watchlist: {e}")
            return []

    def remove_from_watchlist(self, symbol: str) -> bool:
        """Remove symbol from watchlist (mark as inactive)"""
        try:
            query = """
            UPDATE trading_watchlist
            SET is_active = FALSE, updated_at = %s
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
            SELECT symbol, sentiment_score, notes, status, priority,
                   target_entry_price, stop_loss_price, take_profit_price,
                   max_position_size_pct, sector, market_cap, added_date,
                   last_analyzed, is_active
            FROM trading_watchlist
            WHERE symbol = %s
            """
            results = self.execute_query(query, (symbol,))
            return results[0] if results else None
        except psycopg2.Error as e:
            self.logger.error(f"Error getting watchlist info for {symbol}: {e}")
            return None

    def get_trading_watchlist(self) -> List[Dict[str, Any]]:
        """Get current trading watchlist symbols"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT symbol, sentiment_score, news_sentiment, status, priority,
                               target_entry_price, stop_loss_price, take_profit_price,
                               max_position_size_pct, notes, tags, sector, market_cap,
                               added_date, last_analyzed, is_active
                        FROM trading_watchlist
                        WHERE is_active = true
                        ORDER BY priority DESC, symbol
                    """)

                    watchlist = []
                    for row in cursor.fetchall():
                        watchlist.append({
                            'symbol': row['symbol'],
                            'notes': row.get('notes', ''),  # Use notes field from model
                            'sentiment_score': float(row['sentiment_score']) if row['sentiment_score'] else 0.0,
                            'news_sentiment': float(row['news_sentiment']) if row['news_sentiment'] else 0.0,
                            'status': row['status'],
                            'priority': row['priority'],
                            'target_entry_price': float(row['target_entry_price']) if row['target_entry_price'] else None,
                            'stop_loss_price': float(row['stop_loss_price']) if row['stop_loss_price'] else None,
                            'take_profit_price': float(row['take_profit_price']) if row['take_profit_price'] else None,
                            'max_position_size': float(row['max_position_size_pct']) if row['max_position_size_pct'] else 10.0,
                            'sector': row['sector'] or '',
                            'market_cap': row['market_cap'] or '',
                            'added_date': row['added_date'],
                            'last_analyzed': row['last_analyzed']
                        })

                    return watchlist

        except Exception as e:
            self.logger.error(f"Database connection error: {e}")
            self.logger.error(f"Error getting watchlist: {e}")
            return []

    # Helper methods for table creation
    def _ensure_transaction_table(self):
        """Ensure transaction_history table exists"""
        try:
            query = """
            CREATE TABLE IF NOT EXISTS transaction_history (
                transaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                symbol VARCHAR(20) NOT NULL,
                quantity DECIMAL(15, 6) NOT NULL,
                price DECIMAL(15, 6) NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                order_id VARCHAR(50),
                finbert_sentiment DECIMAL(4, 3),
                regime_sentiment DECIMAL(4, 3),
                confidence_score DECIMAL(3, 2),
                xgboost_shap JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            CREATE TABLE IF NOT EXISTS trading_watchlist (
                watchlist_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                symbol VARCHAR(20) UNIQUE NOT NULL,
                status VARCHAR(20) DEFAULT 'WATCHING',
                priority INTEGER DEFAULT 5,
                monitor_frequency INTEGER DEFAULT 60,
                target_entry_price DECIMAL(15, 6),
                stop_loss_price DECIMAL(15, 6),
                take_profit_price DECIMAL(15, 6),
                max_position_size_pct DECIMAL(5, 2) DEFAULT 10.00,
                sentiment_score DECIMAL(4, 3),
                news_sentiment DECIMAL(4, 3),
                technical_score DECIMAL(4, 3),
                risk_score DECIMAL(4, 3),
                preferred_strategies JSONB DEFAULT '[]',
                blacklisted_strategies JSONB DEFAULT '[]',
                notes TEXT,
                tags JSONB DEFAULT '[]',
                sector VARCHAR(50),
                market_cap VARCHAR(20),
                current_holding_id UUID,
                added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_analyzed TIMESTAMP,
                last_price_check TIMESTAMP,
                last_news_check TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                analysis_count INTEGER DEFAULT 0,
                alert_triggered_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            self.execute_update(query)
        except psycopg2.Error as e:
            self.logger.error(f"Error creating watchlist table: {e}")

# Factory function for easy instantiation
def create_trading_db_manager(config: Optional[DatabaseConfig] = None) -> TradingDBManager:
    """Create a TradingDBManager instance with optional configuration"""
    return TradingDBManager(config)
