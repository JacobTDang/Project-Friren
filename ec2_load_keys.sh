#!/bin/bash
# Trading System API Keys Loader
# Usage: source load_keys.sh

echo "Loading Trading API Keys..."

# News API credentials
export NEWS_API_KEY="1f8a9a849194475aa9a9fefd4af69fd2"
export MARKETAUX_API_KEY="MbyHw3epZU19Kvmt18zALcpRBDktGZcZJnSk2vXG"
export ALPHA_VANTAGE_API_KEY="2E6WQYIRNY9539P9"
export FMP_API_KEY="vFwvhPqNo7BLmielN9t8kmFI86qslnNm"

# Reddit credentials
export REDDIT_CLIENT_ID="SX6VxDr7BH5vsGGezRDoow"
export REDDIT_CLIENT_SECRET="VEIygWdy2ASpT34s0Qm1ABMU8zaf5A"
export REDDIT_USER_AGENT="python:trading_sentiment:v1.0 (by /u/jacob)"

# Database credentials
export DATABASE_URL="postgresql://username:password@localhost:5432/trading_db"

# RDS credentials for EC2
export RDS_USERNAME="tamdng"
export RDS_ENDPOINT="frirendb.cjqgoeie6ary.us-east-2.rds.amazonaws.com"
export RDS_PASSWORD="Jtd231198!"
export RDS_PORT="5432"
export RDS_DBNAME="postgres"

# Django secret key
export SECRET_KEY="django-insecure-_+idh036y@2e11^8x99fzd3r1%$29+o4_0q#)h=u%x(atrqk3"

# Verify keys are loaded
echo "API Keys loaded:"
echo "   FMP: ${FMP_API_KEY:0:8}..."
echo "   Marketaux: ${MARKETAUX_API_KEY:0:8}..."
echo "   Alpha Vantage: ${ALPHA_VANTAGE_API_KEY:0:8}..."
echo "   NewsAPI: ${NEWS_API_KEY:0:8}..."
echo "   Reddit: ${REDDIT_CLIENT_ID:0:8}..."
echo "   RDS: ${RDS_ENDPOINT:0:20}..."

# Optional: Add to .bashrc for automatic loading
if ! grep -q "source.*load_keys.sh" ~/.bashrc; then
    echo ""
    echo "To automatically load keys on login, run:"
    echo "   echo 'source $(pwd)/load_keys.sh' >> ~/.bashrc"
fi

echo "Ready for trading system execution!"
