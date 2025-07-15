"""
Monitoring Output Formatter for Friren Trading System
====================================================

Formats position health, portfolio performance, and system monitoring output
to provide real-time visibility into trading system operations.

Target Format:
[POSITION HEALTH] AAPL: 100 shares | value: $17,642.00 | PnL: +0.57% | risk: LOW | action: HOLD
"""

from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass


@dataclass
class PositionHealth:
    """Real position health data - REAL-TIME CALCULATIONS ONLY"""
    symbol: str
    shares: int                      # Actual shares from database/Alpaca
    current_price: float             # Real-time price from Yahoo Finance
    position_value: float            # Live calculated value (shares * current_price)
    unrealized_pnl: float           # Real PnL (current_value - cost_basis)
    unrealized_pnl_pct: float       # Real percentage return
    risk_level: str                 # LOW/MEDIUM/HIGH (VaR-calculated)
    recommended_action: str          # HOLD/REDUCE/INCREASE (algorithm-derived)
    health_score: float             # Composite health (0-100) - NO DEFAULTS
    performance_vs_strategy: float  # Real strategy performance comparison
    volatility_trend: str          # INCREASING/DECREASING/STABLE
    correlation_sp500: float        # Real-time S&P 500 correlation
    sharpe_ratio_current: float     # Current period Sharpe ratio
    max_drawdown_current: float     # Current position max drawdown
    days_held: int                  # Actual days since position opened
    strategy_effectiveness: float   # Strategy performance vs benchmark


@dataclass
class PortfolioSummary:
    """Real portfolio summary data"""
    total_value: float
    daily_pnl: float
    daily_pnl_pct: float
    active_positions: int
    cash_balance: float
    buying_power: float
    market_exposure: float
    sector_diversification: Dict[str, float]
    top_holdings: List[Dict[str, Any]]
    risk_metrics: Dict[str, float]


class MonitoringFormatter:
    """Standardized monitoring output formatter for position health and portfolio metrics"""
    
    @staticmethod
    def format_position_health(symbol: str, shares: int, position_value: float,
                             pnl_pct: float, risk_level: str, action: str) -> str:
        """
        Format position health status
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            shares: Number of shares held
            position_value: Current market value of position
            pnl_pct: Unrealized PnL percentage
            risk_level: Risk classification (LOW/MEDIUM/HIGH)
            action: Recommended action (HOLD/REDUCE/INCREASE)
            
        Returns:
            Formatted position health string matching exact target format
        """
        return f"[POSITION HEALTH] {symbol}: {shares} shares | value: ${position_value:,.2f} | PnL: {pnl_pct:+.2f}% | risk: {risk_level} | action: {action}"
    
    @staticmethod
    def format_portfolio_summary(total_value: float, daily_pnl: float,
                               daily_pnl_pct: float, active_positions: int) -> str:
        """
        Format portfolio summary
        
        Args:
            total_value: Total portfolio value
            daily_pnl: Daily PnL amount
            daily_pnl_pct: Daily PnL percentage
            active_positions: Number of active positions
            
        Returns:
            Formatted portfolio summary
        """
        return f"[PORTFOLIO SUMMARY] Total: ${total_value:,.2f} | Daily PnL: ${daily_pnl:+,.2f} ({daily_pnl_pct:+.2f}%) | Positions: {active_positions}"
    
    @staticmethod
    def format_risk_summary(portfolio_var: float, portfolio_beta: float,
                          max_drawdown: float, sharpe_ratio: float,
                          concentration_risk: float) -> str:
        """
        Format portfolio risk summary
        
        Args:
            portfolio_var: Portfolio Value at Risk
            portfolio_beta: Portfolio beta vs market
            max_drawdown: Maximum drawdown percentage
            sharpe_ratio: Portfolio Sharpe ratio
            concentration_risk: Concentration risk metric
            
        Returns:
            Formatted risk summary
        """
        return f"[RISK SUMMARY] VaR: ${portfolio_var:,.0f} | Beta: {portfolio_beta:.2f} | Max DD: {max_drawdown:.1%} | Sharpe: {sharpe_ratio:.2f} | Concentration: {concentration_risk:.1%}"
    
    @staticmethod
    def format_performance_metrics(symbol: str, return_1d: float, return_1w: float,
                                 return_1m: float, volatility: float,
                                 alpha: float, beta: float) -> str:
        """
        Format position performance metrics
        
        Args:
            symbol: Stock symbol
            return_1d: 1-day return
            return_1w: 1-week return  
            return_1m: 1-month return
            volatility: Annualized volatility
            alpha: Alpha vs benchmark
            beta: Beta vs benchmark
            
        Returns:
            Formatted performance metrics
        """
        return f"[PERFORMANCE] {symbol}: 1D: {return_1d:+.2%} | 1W: {return_1w:+.2%} | 1M: {return_1m:+.2%} | Vol: {volatility:.1%} | Alpha: {alpha:.3f} | Beta: {beta:.2f}"
    
    @staticmethod
    def format_position_alert(symbol: str, alert_type: str, severity: str,
                            current_value: float, threshold: float,
                            recommendation: str) -> str:
        """
        Format position alert
        
        Args:
            symbol: Stock symbol
            alert_type: Type of alert (DRAWDOWN, VOLATILITY, etc.)
            severity: Alert severity (INFO/WARNING/CRITICAL)
            current_value: Current metric value
            threshold: Alert threshold
            recommendation: Recommended action
            
        Returns:
            Formatted position alert
        """
        return f"[POSITION ALERT] {symbol}: {alert_type} {severity} | current: {current_value:.3f} | threshold: {threshold:.3f} | action: {recommendation}"
    
    @staticmethod
    def format_sector_exposure(exposures: Dict[str, float], 
                             target_exposures: Dict[str, float],
                             rebalance_needed: bool) -> str:
        """
        Format sector exposure analysis
        
        Args:
            exposures: Current sector exposure percentages
            target_exposures: Target sector exposure percentages
            rebalance_needed: Whether rebalancing is recommended
            
        Returns:
            Formatted sector exposure summary
        """
        # Show top 3 sectors by exposure
        top_sectors = sorted(exposures.items(), key=lambda x: x[1], reverse=True)[:3]
        sector_str = ", ".join([f"{sector}: {exposure:.1%}" for sector, exposure in top_sectors])
        rebal_str = " | REBALANCE NEEDED" if rebalance_needed else ""
        return f"[SECTOR EXPOSURE] {sector_str}{rebal_str}"
    
    @staticmethod
    def format_system_health(cpu_usage: float, memory_usage: float,
                           disk_usage: float, network_latency: float,
                           redis_status: str, database_status: str) -> str:
        """
        Format system health metrics
        
        Args:
            cpu_usage: CPU usage percentage
            memory_usage: Memory usage percentage
            disk_usage: Disk usage percentage
            network_latency: Network latency in ms
            redis_status: Redis connection status
            database_status: Database connection status
            
        Returns:
            Formatted system health summary
        """
        return f"[SYSTEM HEALTH] CPU: {cpu_usage:.1f}% | Memory: {memory_usage:.1f}% | Disk: {disk_usage:.1f}% | Latency: {network_latency:.0f}ms | Redis: {redis_status} | DB: {database_status}"
    
    @staticmethod
    def format_trading_session(session_start: datetime, trades_executed: int,
                             total_volume: float, success_rate: float,
                             avg_execution_time: float, active_orders: int) -> str:
        """
        Format trading session summary
        
        Args:
            session_start: Session start time
            trades_executed: Number of trades executed
            total_volume: Total trading volume
            success_rate: Trade success rate percentage
            avg_execution_time: Average execution time in ms
            active_orders: Number of active orders
            
        Returns:
            Formatted trading session summary
        """
        session_duration = (datetime.now() - session_start).total_seconds() / 3600  # hours
        return f"[TRADING SESSION] Duration: {session_duration:.1f}h | Trades: {trades_executed} | Volume: ${total_volume:,.0f} | Success: {success_rate:.1f}% | Avg Exec: {avg_execution_time:.0f}ms | Active Orders: {active_orders}"
    
    @staticmethod
    def format_benchmark_comparison(symbol: str, position_return: float,
                                  benchmark_return: float, benchmark_name: str,
                                  relative_performance: float, tracking_error: float) -> str:
        """
        Format benchmark comparison
        
        Args:
            symbol: Stock symbol
            position_return: Position return percentage
            benchmark_return: Benchmark return percentage
            benchmark_name: Name of benchmark
            relative_performance: Relative performance vs benchmark
            tracking_error: Tracking error vs benchmark
            
        Returns:
            Formatted benchmark comparison
        """
        return f"[BENCHMARK] {symbol}: {position_return:+.2%} vs {benchmark_name}: {benchmark_return:+.2%} | Relative: {relative_performance:+.2%} | Tracking Error: {tracking_error:.2%}"
    
    @staticmethod
    def format_liquidity_analysis(symbol: str, avg_daily_volume: int,
                                 position_size: int, days_to_liquidate: float,
                                 bid_ask_spread: float, market_impact_est: float) -> str:
        """
        Format liquidity analysis
        
        Args:
            symbol: Stock symbol
            avg_daily_volume: Average daily trading volume
            position_size: Current position size
            days_to_liquidate: Estimated days to fully liquidate
            bid_ask_spread: Current bid-ask spread
            market_impact_est: Estimated market impact of liquidation
            
        Returns:
            Formatted liquidity analysis
        """
        return f"[LIQUIDITY] {symbol}: Avg Volume: {avg_daily_volume:,} | Position: {position_size:,} shares | Days to Liquidate: {days_to_liquidate:.1f} | Spread: {bid_ask_spread:.3f} | Impact: {market_impact_est:.2%}"
    
    @staticmethod
    def format_correlation_analysis(symbol: str, correlations: Dict[str, float],
                                  correlation_change: Dict[str, float],
                                  diversification_score: float) -> str:
        """
        Format correlation analysis
        
        Args:
            symbol: Stock symbol
            correlations: Correlations with other positions
            correlation_change: Change in correlations over time
            diversification_score: Portfolio diversification score
            
        Returns:
            Formatted correlation analysis
        """
        # Show highest correlation
        if correlations:
            max_corr_symbol, max_corr = max(correlations.items(), key=lambda x: abs(x[1]))
            corr_change = correlation_change.get(max_corr_symbol, 0)
            return f"[CORRELATION] {symbol}: Highest with {max_corr_symbol}: {max_corr:.2f} (Δ{corr_change:+.2f}) | Diversification: {diversification_score:.2f}"
        else:
            return f"[CORRELATION] {symbol}: No significant correlations | Diversification: {diversification_score:.2f}"
    
    @staticmethod
    def format_monitoring_error(component: str, error_type: str,
                              error_message: str, last_successful_update: datetime,
                              impact_level: str) -> str:
        """
        Format monitoring errors
        
        Args:
            component: Component that failed (POSITION_TRACKER, RISK_CALCULATOR, etc.)
            error_type: Type of error
            error_message: Error description
            last_successful_update: Last successful update time
            impact_level: Impact level (LOW/MEDIUM/HIGH)
            
        Returns:
            Formatted error message
        """
        time_since_update = (datetime.now() - last_successful_update).total_seconds() / 60  # minutes
        return f"[MONITORING ERROR] {component}: {error_type} - {error_message} | Last Update: {time_since_update:.0f}m ago | Impact: {impact_level}"
    
    @staticmethod
    def format_monitoring_metrics(component: str, metrics: Dict[str, Any]) -> str:
        """
        Format comprehensive monitoring metrics
        
        Args:
            component: Component name
            metrics: Dictionary containing monitoring metrics
            
        Returns:
            Formatted monitoring metrics summary
        """
        # ZERO-HARDCODING: All metrics must be provided by caller - NO DEFAULTS
        update_frequency = metrics['update_frequency_sec']
        success_rate = metrics['success_rate']
        avg_processing_time = metrics['avg_processing_time_ms']
        data_freshness = metrics['data_freshness_sec']
        
        return f"[MONITORING METRICS] {component}: {update_frequency:.0f}s intervals | {success_rate:.1f}% success | {avg_processing_time:.0f}ms avg | {data_freshness:.0f}s data age"