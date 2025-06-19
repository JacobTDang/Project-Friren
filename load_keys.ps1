# Trading System API Keys Loader - PowerShell Version
# Usage: .\load_keys.ps1

Write-Host "Loading Trading API Keys..." -ForegroundColor Cyan

# News API credentials
$env:NEWS_API_KEY="1f8a9a849194475aa9a9fefd4af69fd2"
$env:MARKETAUX_API_KEY="MbyHw3epZU19Kvmt18zALcpRBDktGZcZJnSk2vXG"
$env:ALPHA_VANTAGE_API_KEY="2E6WQYIRNY9539P9"
$env:FMP_API_KEY="vFwvhPqNo7BLmielN9t8kmFI86qslnNm"

# Reddit credentials
$env:REDDIT_CLIENT_ID="SX6VxDr7BH5vsGGezRDoow"
$env:REDDIT_CLIENT_SECRET="VEIygWdy2ASpT34s0Qm1ABMU8zaf5A"
$env:REDDIT_USER_AGENT="python:trading_sentiment:v1.0 (by /u/jacob)"

# Database credentials
$env:DATABASE_URL="postgresql://username:password@localhost:5432/trading_db"

# RDS credentials for EC2
$env:RDS_USERNAME="tamdng"
$env:RDS_ENDPOINT="frirendb.cjqgoeie6ary.us-east-2.rds.amazonaws.com"
$env:RDS_PASSWORD="Jtd231198!"
$env:RDS_PORT="5432"
$env:RDS_DBNAME="postgres"

# Django secret key
$env:SECRET_KEY="django-insecure-_+idh036y@2e11^8x99fzd3r1%$29+o4_0q#)h=u%x(atrqk3"

# Alpaca API
$env:ALPACA_EMERGENCY_CODE="b00fc166-ad7c-4646-bf66-7858903bad5e"
$env:ALPACA_API_KEY="PKVKIAJVGVM267O6IPEQ"
$env:ALPACA_SECRET="9ca0QWiX6mwCIrhWJkGo2YxenMtbMcZmZx3gbcL3"

# Verify keys are loaded
Write-Host "API Keys loaded:" -ForegroundColor Green
Write-Host "   FMP: $($env:FMP_API_KEY.Substring(0,8))..." -ForegroundColor Yellow
Write-Host "   Marketaux: $($env:MARKETAUX_API_KEY.Substring(0,8))..." -ForegroundColor Yellow
Write-Host "   Alpha Vantage: $($env:ALPHA_VANTAGE_API_KEY.Substring(0,8))..." -ForegroundColor Yellow
Write-Host "   NewsAPI: $($env:NEWS_API_KEY.Substring(0,8))..." -ForegroundColor Yellow
Write-Host "   Reddit: $($env:REDDIT_CLIENT_ID.Substring(0,8))..." -ForegroundColor Yellow
Write-Host "   RDS: $($env:RDS_ENDPOINT.Substring(0,20))..." -ForegroundColor Yellow

Write-Host "   ALPACA_API_KEY: $($env:ALPACA_API_KEY.Substring(0,8))..." -ForegroundColor Yellow
Write-Host "   ALPACA_SECRET: $($env:ALPACA_SECRET.Substring(0,8))..." -ForegroundColor Yellow

Write-Host "Ready for trading system execution!" -ForegroundColor Green

# Optional: Test the APIs
$testAPIs = Read-Host "Test API connections? (y/n)"
if ($testAPIs -eq "y") {
    Write-Host " Testing API connections..." -ForegroundColor Cyan

    # Test FMP
    try {
        $response = Invoke-RestMethod -Uri "https://financialmodelingprep.com/api/v3/fmp/articles?apikey=$env:FMP_API_KEY&page=0&size=1"
        Write-Host "   FMP: Connected" -ForegroundColor Green
    } catch {
        Write-Host "   FMP: Failed" -ForegroundColor Red
    }

    # Test Marketaux
    try {
        $response = Invoke-RestMethod -Uri "https://api.marketaux.com/v1/news/all?api_token=$env:MARKETAUX_API_KEY&limit=1"
        Write-Host "   Marketaux: Connected" -ForegroundColor Green
    } catch {
        Write-Host "   Marketaux: Failed" -ForegroundColor Red
    }

    # Test Alpha Vantage
    try {
        $response = Invoke-RestMethod -Uri "https://www.alphavantage.co/query?function=NEWS_SENTIMENT&apikey=$env:ALPHA_VANTAGE_API_KEY&limit=1"
        Write-Host "   Alpha Vantage: Connected" -ForegroundColor Green
    } catch {
        Write-Host "   Alpha Vantage: Failed" -ForegroundColor Red
    }

    # Test NewsAPI
    try {
        $response = Invoke-RestMethod -Uri "https://newsapi.org/v2/top-headlines?category=business&country=us&pageSize=1&apiKey=$env:NEWS_API_KEY"
        Write-Host "   NewsAPI: Connected" -ForegroundColor Green
    } catch {
        Write-Host "   NewsAPI:  Failed" -ForegroundColor Red
    }
}
