# Multi-Market Stock Screener

A Python-based stock screener that scans U.S., Canadian, and Indian equities using a rule-based momentum and catalyst framework.

## Markets Covered

- United States — Nasdaq / NYSE
- Canada — TSX / TSXV
- India — NSE / BSE

## Screening Criteria

The screener evaluates stocks using five main rules:

1. Daily price change >= 10%
2. Relative Volume (RVOL) >= 5x
3. Direct identifiable catalyst or news event
4. Market-specific stock price range
5. Public float <= 20 million shares

## Results

Stocks are grouped into:

- **Qualified (5/5)** — meets all five criteria
- **Near Match (4/5)** — misses one criterion but remains within tolerance
- **Review / Unverified** — potentially qualifies but requires additional data verification

## Features

- Multi-market screening from one Python program
- 20-day relative volume calculations
- Catalyst/news verification
- Fresh vs repeated news detection
- Liquidity and turnover checks
- Market-specific thresholds
- Error handling
- Automated CSV output

For Indian equities, the screener uses official NSE/BSE exchange data for quantitative screening and prioritizes official exchange announcements for catalyst verification.

## Getting Started

### 1. Install Python

Download Python from:

https://www.python.org/

On Windows, IDLE is included with Python.

### 2. Install Required Packages

Open Command Prompt and run:

```bash
py -m pip install pandas yfinance requests lxml openpyxl
