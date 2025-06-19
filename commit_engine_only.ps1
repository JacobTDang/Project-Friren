# Friren Trading Engine - Commit ONLY the Core Engine
# This script commits ONLY the Friren_V1/ directory and ignores all test files

Write-Host "Committing ONLY Friren_V1 Trading Engine..."

# Set Git path
$gitPath = "C:\Program Files\Git\bin\git.exe"

# Create comprehensive .gitignore to exclude everything except Friren_V1/
Write-Host "Creating .gitignore to exclude test files and sensitive data..."
$gitignoreContent = @"
# Environment Variables - NEVER COMMIT
.env
*.env
.env.local
.env.production
*.key
*_keys.py
api_keys.py

# Test Files - DO NOT COMMIT
test_*.py
*_test.py
demo_*.py
comprehensive_*.py
enhanced_*.py
priority_queue_demo.py
simple_paper_trading_test.py
validate_test_setup.py
final_system_status.py
system_cycle_demo.py
queue_based_*.py

# Log Files
*.log
logs/
Friren_V1/logs/

# Reports and Test Outputs
*.html
*.json
e2e_test_report_*
demo_smoke_results.*
comprehensive_test_report.json
comprehensive_e2e_test.log
e2e_test_runner.log

# Scripts and Keys
load_keys.ps1
ec2_load_keys.sh
commit_*.ps1
emergency_cleanup.ps1

# Documentation and Notes
notes_*.txt
directory_architecture.txt
project_changes_summary.txt
~bashrc

# Cache and IDE
__pycache__/
*.pyc
*.pyo
.cursor/
.vscode/
.idea/

# OS Files
.DS_Store
Thumbs.db

# Virtual Environment
venv/
env/

# Prompts Directory
prompts/

# Ignore ALL root-level files except core engine
/*
!/Friren_V1/
!/.gitignore
!/README.md
"@

$gitignoreContent | Out-File -FilePath ".gitignore" -Encoding UTF8

# Add .gitignore first
Write-Host "Adding .gitignore..."
& $gitPath add .gitignore

# Add ONLY the core Friren_V1 directory (excluding test subdirectories)
Write-Host "Adding Friren_V1 core engine files..."

# Core configuration
& $gitPath add Friren_V1/config/

# Infrastructure
& $gitPath add Friren_V1/infrastructure/

# Model maintainer
& $gitPath add Friren_V1/model_maintainer/

# Multiprocess infrastructure
& $gitPath add Friren_V1/multiprocess_infrastructure/

# Core trading engine files (excluding test directories)
& $gitPath add Friren_V1/trading_engine/__init__.py
& $gitPath add Friren_V1/trading_engine/__main__.py
& $gitPath add Friren_V1/trading_engine/orchestrator.py
& $gitPath add Friren_V1/trading_engine/per_stock_orchestrator.py

# Backtesting (exclude test files)
& $gitPath add Friren_V1/trading_engine/backtesting/__init__.py
& $gitPath add Friren_V1/trading_engine/backtesting/bolinger_bt.py
& $gitPath add Friren_V1/trading_engine/backtesting/rsi_backtest.py
& $gitPath add Friren_V1/trading_engine/backtesting/pairs_*.py
& $gitPath add Friren_V1/trading_engine/backtesting/PCA_bt.py
& $gitPath add Friren_V1/trading_engine/backtesting/jdiffusion_bt.py

# Data modules (exclude test files)
& $gitPath add Friren_V1/trading_engine/data/__init__.py
& $gitPath add Friren_V1/trading_engine/data/data_utils.py
& $gitPath add Friren_V1/trading_engine/data/price_data.py
& $gitPath add Friren_V1/trading_engine/data/yahoo_price.py

# News data
& $gitPath add Friren_V1/trading_engine/data/news/__init__.py
& $gitPath add Friren_V1/trading_engine/data/news/news_api.py
& $gitPath add Friren_V1/trading_engine/data/news/fmp_api.py
& $gitPath add Friren_V1/trading_engine/data/news/marketaux_api.py
& $gitPath add Friren_V1/trading_engine/data/news/news_aggregator.py
& $gitPath add Friren_V1/trading_engine/data/news/news_processor.py
& $gitPath add Friren_V1/trading_engine/data/news/sentiment_analyzer.py
& $gitPath add Friren_V1/trading_engine/data/news/reddit_scraper.py

# Portfolio manager
& $gitPath add Friren_V1/trading_engine/portfolio_manager/

# Regime detection
& $gitPath add Friren_V1/trading_engine/regime_detection/__init__.py
& $gitPath add Friren_V1/trading_engine/regime_detection/entropy_regime_detector.py
& $gitPath add Friren_V1/trading_engine/regime_detection/regime_detector.py

# Sentiment analysis
& $gitPath add Friren_V1/trading_engine/sentiment/

# Core Django files
& $gitPath add Friren_V1/manage.py
& $gitPath add Friren_V1/validate_requirements.py

# Commit the core engine
& $gitPath commit -m "FEATURE: Complete Friren V1 Trading Engine

Core Features:
- Rate-limited news APIs (NewsAPI, FMP, Marketaux)
- Windows-compatible orchestrator systems
- Comprehensive portfolio management
- FinBERT sentiment analysis integration
- Multi-strategy trading execution
- Risk management and position health monitoring
- Paper trading with real-time decision making
- Enhanced backtesting capabilities
- Django integration for database management

Technical Improvements:
- Multiprocessing infrastructure for Windows
- Comprehensive error handling and logging
- API rate limiting protection
- Modular architecture for easy extension
- Production-ready configuration management"

# Push to GitHub
Write-Host "Pushing core engine to GitHub..."
& $gitPath push origin main

Write-Host ""
Write-Host "SUCCESS: Core Friren V1 Trading Engine committed to GitHub"
Write-Host "EXCLUDED: All test files, demos, logs, and sensitive data"
Write-Host ""
Write-Host "Repository now contains:"
Write-Host "- Core trading engine (Friren_V1/)"
Write-Host "- Rate limiting system"
Write-Host "- Portfolio management"
Write-Host "- Sentiment analysis"
Write-Host "- Windows orchestrators"
Write-Host "- Production configuration"
