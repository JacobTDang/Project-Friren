# Enhanced Decision Engine Rules

## Position-Aware Decision Logic with Sentiment-Based Sizing

### Rule 1: Position State Evaluation
```
IF already holding position for symbol:
    current_position_pct = get_current_position_size(symbol)

    IF recommendation == BUY:
        IF sentiment_score >= 0.7 AND current_position_pct < 15%:
            # High sentiment allows adding to position
            additional_size = min(0.03, 0.15 - current_position_pct)  # Add up to 3% more
            ACTION = "BUY_MORE"
            target_position_pct = current_position_pct + additional_size
        ELSE:
            # Normal buy signal becomes hold when already positioned
            ACTION = "CONTINUE_HOLDING"
            target_position_pct = current_position_pct

    ELIF recommendation == SELL:
        ACTION = "SELL"
        target_position_pct = 0.0  # Close position

    ELSE:  # recommendation == HOLD
        ACTION = "CONTINUE_HOLDING"
        target_position_pct = current_position_pct

ELSE:  # No current position
    IF recommendation == BUY:
        ACTION = "BUY"
        target_position_pct = calculate_initial_position_size(confidence, sentiment_score)
    ELSE:
        ACTION = "HOLD"
        target_position_pct = 0.0
```

### Rule 2: Sentiment-Based Position Sizing
```
def calculate_initial_position_size(confidence, sentiment_score):
    # Base size from confidence (3-12% range)
    base_size = 0.03 + (confidence - 0.5) * 0.18  # 3% at 50% confidence, 12% at 100%

    # Sentiment modifier (0.8x to 1.2x)
    sentiment_modifier = 1.0 + (sentiment_score * 0.2)
    sentiment_modifier = max(0.8, min(1.2, sentiment_modifier))

    # Calculate final size
    target_size = base_size * sentiment_modifier

    # Enforce limits: 3% minimum, 15% maximum
    return max(0.03, min(0.15, target_size))

def calculate_additional_size(current_size, sentiment_score):
    # Only add more if sentiment is very positive (>= 0.7)
    if sentiment_score >= 0.7:
        # Add 1-3% based on sentiment strength
        additional = 0.01 + (sentiment_score - 0.7) * 0.067  # 1% at 0.7, 3% at 1.0
        return min(additional, 0.15 - current_size)  # Don't exceed 15% total
    return 0.0
```

### Rule 3: Position Sizer Integration
```
# Modify your PurePositionSizer to check current holdings
def enhanced_size_calculation(symbol, recommendation, confidence, sentiment_score):
    current_position_pct = position_sizer.get_current_size(symbol)

    if current_position_pct > 0:
        # Already holding - apply position-aware logic
        if recommendation == "BUY" and sentiment_score >= 0.7:
            target_pct = current_position_pct + calculate_additional_size(current_position_pct, sentiment_score)
            action = "BUY_MORE"
        elif recommendation == "SELL":
            target_pct = 0.0
            action = "SELL"
        else:
            target_pct = current_position_pct
            action = "CONTINUE_HOLDING"
    else:
        # No position - normal logic
        if recommendation == "BUY":
            target_pct = calculate_initial_position_size(confidence, sentiment_score)
            action = "BUY"
        else:
            target_pct = 0.0
            action = "HOLD"

    return position_sizer.size_up(symbol, target_pct), action
```

### Rule 4: Risk Manager Updates
```
# Add to risk_manager.py limits
'max_position_addition_pct': 0.03,    # Max 3% addition per trade
'min_sentiment_for_addition': 0.7,    # Minimum sentiment to add to position
'max_total_position_pct': 0.15,       # Absolute maximum per symbol

# Add validation in _validate_position_size
def _validate_position_addition(self, symbol: str, current_pct: float,
                               target_pct: float, sentiment_score: float) -> Tuple[bool, str]:
    addition_amount = target_pct - current_pct

    if addition_amount > self.limits['max_position_addition_pct']:
        return False, f"Position addition too large ({addition_amount:.1%} > {self.limits['max_position_addition_pct']:.1%})"

    if sentiment_score < self.limits['min_sentiment_for_addition']:
        return False, f"Sentiment too low for position addition ({sentiment_score:.2f} < {self.limits['min_sentiment_for_addition']:.2f})"

    if target_pct > self.limits['max_total_position_pct']:
        return False, f"Total position would exceed limit ({target_pct:.1%} > {self.limits['max_total_position_pct']:.1%})"

    return True, "Position addition approved"
```

## Implementation Steps for Real Paper Trading

### Step 1: Fix Module Structure
```bash
# Problem: portfolio_manager.py is a directory with .py extension
# Solution: Add proper __init__.py files and fix import paths

# Create proper import structure:
cd Friren_V1/trading_engine/
# Ensure portfolio_manager directory has working __init__.py
# Test import with: python -c "from portfolio_manager.decision_engine.execution_orchestrator import ExecutionOrchestrator"
```

### Step 2: Test ExecutionOrchestrator Connection
```python
# Test script to verify Alpaca and database connections
def test_execution_orchestrator():
    from portfolio_manager.decision_engine.execution_orchestrator import ExecutionOrchestrator
    from portfolio_manager.tools.alpaca_interface import SimpleAlpacaInterface, AlpacaConfig
    from portfolio_manager.tools.db_manager import TradingDBManager

    # Initialize components
    alpaca = SimpleAlpacaInterface(AlpacaConfig.from_environment())
    db = TradingDBManager("test")
    orchestrator = ExecutionOrchestrator(alpaca_interface=alpaca, db_manager=db)

    # Test connections
    account = alpaca.get_account_info()
    print(f"Account value: ${account.portfolio_value:,.2f}")

    holdings = db.get_holdings(active_only=True)
    print(f"Active holdings: {len(holdings)}")

    return "ExecutionOrchestrator ready for real paper trading"
```

### Step 3: Replace Mock Trading Logic
```python
# In your orchestrator, replace current mock logic with:
def execute_real_paper_trades(self, decisions):
    for symbol, decision in decisions.items():
        try:
            # Get current position
            current_size = self.position_sizer.get_current_size(symbol)

            # Apply enhanced decision logic
            size_calc, action = enhanced_size_calculation(
                symbol,
                decision['recommendation'],
                decision['confidence'],
                decision['sentiment_score']
            )

            # Create risk validation request
            risk_request = RiskValidationRequest(
                symbol=symbol,
                side=OrderSide.BUY if action in ["BUY", "BUY_MORE"] else OrderSide.SELL,
                quantity=size_calc.shares_to_trade,
                strategy_name=decision['strategy'],
                strategy_confidence=decision['confidence']
            )

            # Validate through risk manager
            risk_result = self.risk_manager.validate_risk_comprehensive(risk_request)

            if risk_result.approved:
                # Execute through ExecutionOrchestrator
                execution_result = self.execution_orchestrator.execute_approved_decision(
                    risk_validation=risk_result,
                    strategy_name=decision['strategy'],
                    strategy_confidence=decision['confidence']
                )

                if execution_result.was_successful:
                    self.logger.info(f"REAL PAPER TRADE EXECUTED: {execution_result.execution_summary}")
                    # Trade logged to database automatically by ExecutionOrchestrator
                else:
                    self.logger.warning(f"Trade failed: {execution_result.error_message}")
            else:
                self.logger.info(f"Trade blocked by risk manager: {risk_result.rejection_reason}")

        except Exception as e:
            self.logger.error(f"Error executing trade for {symbol}: {e}")
```

### Step 4: Database Transaction Verification
```python
# Add logging verification to ensure database writes
def verify_database_logging(self):
    try:
        # Get recent transactions
        recent_transactions = self.db_manager.get_recent_transactions(limit=10)

        # Get current holdings
        current_holdings = self.db_manager.get_holdings(active_only=True)

        self.logger.info(f"Recent transactions: {len(recent_transactions)}")
        self.logger.info(f"Active holdings: {len(current_holdings)}")

        # Log each holding with strategy assignment
        for holding in current_holdings:
            self.logger.info(f"Holding: {holding['symbol']} - {holding['net_quantity']} shares - Strategy: {holding.get('strategy_name', 'Unknown')}")

        return True
    except Exception as e:
        self.logger.error(f"Database verification failed: {e}")
        return False
```

### Step 5: Strategy Performance Monitoring
```python
# Add strategy performance tracking
def monitor_strategy_performance(self):
    try:
        # Get strategy performance from ExecutionOrchestrator
        strategy_performance = self.execution_orchestrator.get_strategy_performance()

        self.logger.info("Strategy Performance Summary:")
        for strategy, metrics in strategy_performance.items():
            success_rate = metrics.get('success_rate', 0.0)
            total_trades = metrics.get('executions', 0)
            avg_return = metrics.get('avg_return_pct', 0.0)

            self.logger.info(f"{strategy}: {success_rate:.1%} success ({total_trades} trades) - Avg return: {avg_return:+.2%}")

        # Get execution orchestrator status
        execution_status = self.execution_orchestrator.get_execution_status()

        self.logger.info(f"Total executions: {execution_status.get('total_executions', 0)}")
        self.logger.info(f"Overall success rate: {execution_status.get('success_rate', 0.0):.1%}")
        self.logger.info(f"Average execution time: {execution_status.get('average_execution_time_ms', 0.0):.0f}ms")

        return strategy_performance
    except Exception as e:
        self.logger.error(f"Strategy monitoring failed: {e}")
        return {}
```

## Final Integration Checklist

### Code Integration
- [ ] Add position-aware decision logic to decision engine
- [ ] Update position sizer with sentiment-based sizing
- [ ] Enhance risk manager with position addition limits
- [ ] Replace mock trading with ExecutionOrchestrator calls
- [ ] Add database verification logging
- [ ] Implement strategy performance monitoring

### Testing Checklist
- [ ] Import paths work for ExecutionOrchestrator
- [ ] Alpaca paper trading connection verified
- [ ] PostgreSQL database connection verified
- [ ] Risk manager approves position additions correctly
- [ ] ExecutionOrchestrator executes trades and logs to database
- [ ] Strategy assignments are recorded properly
- [ ] Performance tracking shows accurate metrics

### Operational Checklist
- [ ] Environment variables loaded (ALPACA_API_KEY, ALPACA_SECRET, RDS_*)
- [ ] Risk limits configured (15% max position, 3% max addition)
- [ ] Sentiment thresholds set (0.7 minimum for position additions)
- [ ] Database tables exist and are accessible
- [ ] Alpaca paper trading account funded and accessible
- [ ] Logging configured to capture all trade executions
