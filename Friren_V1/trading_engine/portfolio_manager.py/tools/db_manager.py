"""
Trading Database Manager for Portfolio Manager

Lightweight SQL tool that reuses Django models for schema awareness
while providing direct database access for multiprocess trading system.
"""

import os
import sys
import django
import psycopg2
import psycopg2.extras
import logging
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from decimal import Decimal
from contextlib import contextmanager
from dataclasses import dataclass
import json

# Configure Django for model access only
if not django.conf.settings.configured:
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
    django.setup()

# Import Django models for schema awareness
from infrastructure.database.models import TransactionHistory, CurrentHoldings, MLFeatures

@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str
    port: str
    dbname: str
    username: str
    password: str

    @classmethod
    def from_env(cls):
        return cls(
            host=os.getenv('RDS_ENDPOINT'),
            port=os.getenv('RDS_PORT', '5432'),
            dbname=os.getenv('RDS_DBNAME'),
            username=os.getenv('RDS_USERNAME'),
            password=os.getenv('RDS_PASSWORD')
        )

class DatabaseError(Exception):
    """Custom database error for trading system"""
    pass

class TradingDBManager:
    """
    Lightweight database manager for trading operations

    Features:
    - Simple CRUD operations
    - Django model schema awareness
    - Process-specific error handling
    - Connection pooling
    - Fail-fast for critical processes
    """

    def __init__(self, process_name: str = "unknown"):
        self.process_name = process_name
        self.is_critical_process = process_name == "market_decision_engine"
        self.config = DatabaseConfig.from_env()
        self.logger = logging.getLogger(f"trading_db.{process_name}")

        # Test connection on initialization
        self._test_connection()

    def _test_connection(self):
        """Test database connectivity"""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
            self.logger.info(f"Database connection successful for {self.process_name}")
        except Exception as e:
            error_msg = f"Database connection failed for {self.process_name}: {e}"
            self.logger.error(error_msg)
            if self.is_critical_process:
                raise DatabaseError(error_msg)

    @contextmanager
    def _get_connection(self):
        """Get database connection with automatic cleanup"""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                dbname=self.config.dbname,
                user=self.config.username,
                password=self.config.password,
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            conn.autocommit = True
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                conn.close()

    def _execute_with_retry(self, query: str, params: tuple = None, retries: int = 3) -> List[Dict]:
        """Execute query with retry logic and process-specific error handling"""
        last_error = None

        for attempt in range(retries):
            try:
                with self._get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(query, params)

                        # Return results for SELECT queries
                        if query.strip().upper().startswith('SELECT'):
                            return cursor.fetchall()
                        else:
                            return [{"affected_rows": cursor.rowcount}]

            except Exception as e:
                last_error = e
                self.logger.warning(f"Database attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    continue

        # All retries failed
        error_msg = f"Database operation failed after {retries} attempts: {last_error}"
        self.logger.error(error_msg)

        if self.is_critical_process:
            raise DatabaseError(error_msg)
        else:
            self.logger.warning(f"Non-critical process {self.process_name} continuing despite DB error")
            return []

    # ========== TRANSACTION HISTORY OPERATIONS ==========

    def insert_transaction(self, symbol: str, quantity: float, price: float,
                          timestamp: datetime, order_id: str = None,
                          finbert_sentiment: float = None, regime_sentiment: float = None,
                          confidence_score: float = None, xgboost_shap: dict = None) -> bool:
        """Insert new transaction record"""
        query = """
        INSERT INTO transaction_history
        (symbol, quantity, price, timestamp, order_id, finbert_sentiment,
         regime_sentiment, confidence_score, xgboost_shap)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        params = (
            symbol, Decimal(str(quantity)), Decimal(str(price)), timestamp,
            order_id,
            Decimal(str(finbert_sentiment)) if finbert_sentiment else None,
            Decimal(str(regime_sentiment)) if regime_sentiment else None,
            Decimal(str(confidence_score)) if confidence_score else None,
            json.dumps(xgboost_shap) if xgboost_shap else None
        )

        result = self._execute_with_retry(query, params)
        return len(result) > 0 and result[0].get('affected_rows', 0) > 0

    def get_transactions(self, symbol: str = None, limit: int = None) -> List[Dict]:
        """Get transaction history"""
        query = "SELECT * FROM transaction_history"
        params = None

        if symbol:
            query += " WHERE symbol = %s"
            params = (symbol,)

        query += " ORDER BY timestamp DESC"

        if limit:
            query += f" LIMIT {limit}"

        return self._execute_with_retry(query, params)

    # ========== CURRENT HOLDINGS OPERATIONS ==========

    def get_holdings(self, symbol: str = None, active_only: bool = True) -> List[Dict]:
        """Get current holdings"""
        query = "SELECT * FROM current_holdings"
        conditions = []
        params = []

        if symbol:
            conditions.append("symbol = %s")
            params.append(symbol)

        if active_only:
            conditions.append("is_active = %s")
            params.append(True)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY symbol"

        return self._execute_with_retry(query, tuple(params))

    def upsert_holding(self, symbol: str, net_quantity: float, avg_cost_basis: float,
                      total_invested: float, realized_pnl: float = 0,
                      first_purchase_date: datetime = None,
                      last_transaction_date: datetime = None,
                      number_of_transactions: int = 1) -> bool:
        """Insert or update holding record"""

        # Check if holding exists
        existing = self.get_holdings(symbol=symbol, active_only=False)

        if existing:
            # Update existing
            query = """
            UPDATE current_holdings
            SET net_quantity = %s, avg_cost_basis = %s, total_invested = %s,
                realized_pnl = %s, last_transaction_date = %s,
                number_of_transactions = %s, is_active = %s
            WHERE symbol = %s
            """
            is_active = float(net_quantity) != 0.0
            params = (
                Decimal(str(net_quantity)), Decimal(str(avg_cost_basis)),
                Decimal(str(total_invested)), Decimal(str(realized_pnl)),
                last_transaction_date or datetime.now(),
                number_of_transactions, is_active, symbol
            )
        else:
            # Insert new
            query = """
            INSERT INTO current_holdings
            (symbol, net_quantity, avg_cost_basis, total_invested, realized_pnl,
             first_purchase_date, last_transaction_date, number_of_transactions)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = (
                symbol, Decimal(str(net_quantity)), Decimal(str(avg_cost_basis)),
                Decimal(str(total_invested)), Decimal(str(realized_pnl)),
                first_purchase_date or datetime.now(),
                last_transaction_date or datetime.now(),
                number_of_transactions
            )

        result = self._execute_with_retry(query, params)
        return len(result) > 0 and result[0].get('affected_rows', 0) > 0

    def delete_holding(self, symbol: str) -> bool:
        """Delete holding record (use carefully!)"""
        query = "DELETE FROM current_holdings WHERE symbol = %s"
        result = self._execute_with_retry(query, (symbol,))
        return len(result) > 0 and result[0].get('affected_rows', 0) > 0

    # ========== ML FEATURES OPERATIONS ==========

    def bulk_insert_ml_features(self, features_list: List[Dict]) -> int:
        """Bulk insert ML features for training data"""
        if not features_list:
            return 0

        # Build bulk insert query
        query = """
        INSERT INTO ml_features
        (symbol, timestamp, price, volume, rsi_14, bb_upper, bb_lower, bb_middle,
         sma_20, sma_50, ema_12, ema_26, finbert_sentiment, regime_sentiment,
         news_sentiment, vix_level, market_regime, strategy_used,
         strategy_signal_strength, model_confidence, decision_reasoning, custom_features)
        VALUES %s
        """

        # Prepare values
        values = []
        for feature in features_list:
            values.append((
                feature.get('symbol'),
                feature.get('timestamp'),
                Decimal(str(feature.get('price', 0))),
                feature.get('volume'),
                Decimal(str(feature.get('rsi_14'))) if feature.get('rsi_14') else None,
                Decimal(str(feature.get('bb_upper'))) if feature.get('bb_upper') else None,
                Decimal(str(feature.get('bb_lower'))) if feature.get('bb_lower') else None,
                Decimal(str(feature.get('bb_middle'))) if feature.get('bb_middle') else None,
                Decimal(str(feature.get('sma_20'))) if feature.get('sma_20') else None,
                Decimal(str(feature.get('sma_50'))) if feature.get('sma_50') else None,
                Decimal(str(feature.get('ema_12'))) if feature.get('ema_12') else None,
                Decimal(str(feature.get('ema_26'))) if feature.get('ema_26') else None,
                Decimal(str(feature.get('finbert_sentiment'))) if feature.get('finbert_sentiment') else None,
                Decimal(str(feature.get('regime_sentiment'))) if feature.get('regime_sentiment') else None,
                Decimal(str(feature.get('news_sentiment'))) if feature.get('news_sentiment') else None,
                Decimal(str(feature.get('vix_level'))) if feature.get('vix_level') else None,
                feature.get('market_regime'),
                feature.get('strategy_used'),
                Decimal(str(feature.get('strategy_signal_strength'))) if feature.get('strategy_signal_strength') else None,
                Decimal(str(feature.get('model_confidence'))) if feature.get('model_confidence') else None,
                feature.get('decision_reasoning'),
                json.dumps(feature.get('custom_features')) if feature.get('custom_features') else None
            ))

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    psycopg2.extras.execute_values(cursor, query, values, template=None, page_size=100)
                    return cursor.rowcount
        except Exception as e:
            error_msg = f"Bulk insert failed: {e}"
            self.logger.error(error_msg)
            if self.is_critical_process:
                raise DatabaseError(error_msg)
            return 0

    def get_ml_features(self, symbol: str = None, strategy: str = None,
                       limit: int = 1000) -> List[Dict]:
        """Get ML features for analysis"""
        query = "SELECT * FROM ml_features"
        conditions = []
        params = []

        if symbol:
            conditions.append("symbol = %s")
            params.append(symbol)

        if strategy:
            conditions.append("strategy_used = %s")
            params.append(strategy)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += f" ORDER BY timestamp DESC LIMIT {limit}"

        return self._execute_with_retry(query, tuple(params))

    # ========== UTILITY METHODS ==========

    def get_portfolio_summary(self) -> Dict:
        """Get portfolio summary for health monitoring"""
        query = """
        SELECT
            COUNT(*) as total_positions,
            SUM(CASE WHEN net_quantity > 0 THEN 1 ELSE 0 END) as long_positions,
            SUM(CASE WHEN net_quantity < 0 THEN 1 ELSE 0 END) as short_positions,
            SUM(total_invested) as total_invested,
            SUM(realized_pnl) as total_realized_pnl
        FROM current_holdings
        WHERE is_active = true
        """

        result = self._execute_with_retry(query)
        return result[0] if result else {}

    def health_check(self) -> Dict:
        """Database health check for monitoring"""
        try:
            start_time = datetime.now()
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM current_holdings")
                    holdings_count = cursor.fetchone()['count']

            response_time = (datetime.now() - start_time).total_seconds()

            return {
                "status": "healthy",
                "process": self.process_name,
                "response_time_ms": response_time * 1000,
                "holdings_count": holdings_count,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "process": self.process_name,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }


# ========== USAGE EXAMPLES ==========

if __name__ == "__main__":
    # Example usage for different processes

    # Market Decision Engine (Critical Process)
    decision_db = TradingDBManager("market_decision_engine")

    # Record a trade
    success = decision_db.insert_transaction(
        symbol="AAPL",
        quantity=100.0,
        price=150.50,
        timestamp=datetime.now(),
        order_id="ORD123456",
        finbert_sentiment=0.25,
        confidence_score=0.85
    )

    if success:
        # Update holdings
        decision_db.upsert_holding(
            symbol="AAPL",
            net_quantity=100.0,
            avg_cost_basis=150.50,
            total_invested=15050.0,
            last_transaction_date=datetime.now()
        )

    # Position Health Monitor (Non-Critical Process)
    health_db = TradingDBManager("position_health_monitor")
    holdings = health_db.get_holdings(active_only=True)
    portfolio_summary = health_db.get_portfolio_summary()

    # Strategy Analyzer (Non-Critical Process)
    strategy_db = TradingDBManager("strategy_analyzer")

    # Bulk insert ML features after market close
    ml_features = [
        {
            "symbol": "AAPL",
            "timestamp": datetime.now(),
            "price": 150.50,
            "rsi_14": 65.2,
            "strategy_used": "momentum",
            "strategy_signal_strength": 0.8,
            "market_regime": "BULL_MARKET"
        },
        # ... more features
    ]

    inserted_count = strategy_db.bulk_insert_ml_features(ml_features)
    print(f"Inserted {inserted_count} ML feature records")
