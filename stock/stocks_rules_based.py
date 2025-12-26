# ---------------------------------------
# Rule-Based Stock Trader (Paper Trading)
# ---------------------------------------

import yfinance as yf  # Library to fetch stock price data
import pandas as pd    # Library for data manipulation
from alpaca_trade_api.rest import REST

API_KEY = "PKNQDR5OZ3ZOMN463CGKHX6X5A"       # copy from Alpaca dashboard
API_SECRET = "Bzqmzcg6K9h8CqxDynnfjGic8o3iBWMkJ3CknSHwBGPR" # copy from Alpaca dashboard
BASE_URL = "https://paper-api.alpaca.markets/v2"  # important: paper trading URL

api = REST(API_KEY, API_SECRET, base_url=BASE_URL)

# -------------------------------
# 1. Fetch historical stock data
# -------------------------------
ticker = "NVDA"  # Stock symbol you want to trade
# Download daily stock prices from Yahoo Finance starting Jan 1, 2020
data = yf.download(ticker, start="2020-01-01")

# -------------------------------
# 2. Calculate technical indicators
# -------------------------------
# Short-term Simple Moving Average (SMA) over 5 days
data['SMA5'] = data['Close'].rolling(5).mean()

# Long-term Simple Moving Average (SMA) over 20 days
data['SMA20'] = data['Close'].rolling(20).mean()

# Drop initial rows with missing SMA values
data = data.dropna()

# -------------------------------
# 3. Define backtesting function
# -------------------------------
def backtest_rules(data, initial_cash=10000):
    """
    Simulates paper trading based on SMA crossover rules.

    Args:
        data (DataFrame): Stock data with SMA5 and SMA20 columns
        initial_cash (float): Starting capital for trading

    Returns:
        final_value (float): Total portfolio value at the end
    """
    cash = float(initial_cash)  # Start with cash in dollars
    shares = 0.0                # Start with zero shares

    # Loop through each day in the dataset, starting from the second row
    for i in range(1, len(data)):
        price = float(data['Close'].iloc[i])  # Current day's closing price

        # -------------------------------
        # Buy Rule: SMA5 crosses above SMA20
        # -------------------------------
        # If yesterday SMA5 < SMA20 AND today SMA5 > SMA20 → BUY signal
        if data['SMA5'].iloc[i-1] < data['SMA20'].iloc[i-1] and data['SMA5'].iloc[i] > data['SMA20'].iloc[i]:
            if cash > 0:  # Only buy if we have cash
                shares = cash / price  # Buy as many shares as possible
                cash = 0.0             # All cash is now invested
                # print(f"BUY at {price:.2f}")  # Optional: print each trade

        # -------------------------------
        # Sell Rule: SMA5 crosses below SMA20
        # -------------------------------
        # If yesterday SMA5 > SMA20 AND today SMA5 < SMA20 → SELL signal
        elif data['SMA5'].iloc[i-1] > data['SMA20'].iloc[i-1] and data['SMA5'].iloc[i] < data['SMA20'].iloc[i]:
            if shares > 0:  # Only sell if we own shares
                cash = shares * price  # Sell all shares
                shares = 0.0           # No shares left
                # print(f"SELL at {price:.2f}")  # Optional: print each trade

    # -------------------------------
    # Calculate final portfolio value
    # -------------------------------
    # Cash + value of any remaining shares at last closing price
    final_value = cash + shares * float(data['Close'].iloc[-1])
    return final_value

# -------------------------------
# 4. Run backtest and show results
# -------------------------------
final_portfolio = backtest_rules(data)  # Run the paper trading simulation
print(f"Final portfolio value for {ticker}: ${final_portfolio:.2f}")  # Display final value
