# ---------------------------------------
# ML Stock Predictor + Backtest (Paper Trading)
# ---------------------------------------

import yfinance as yf  # Library to fetch stock data
import pandas as pd  # Library for data manipulation
from sklearn.ensemble import RandomForestClassifier  # ML model
from sklearn.model_selection import train_test_split  # Split data into training/test
from sklearn.metrics import accuracy_score  # Measure model accuracy


# -------------------------------
# 1. Function to train ML model
# -------------------------------
def train_model(ticker):
    """
    Trains a machine learning model to predict next-day stock movement (up/down)
    for a given stock ticker.

    Args:
        ticker (str): Stock symbol, e.g., 'AAPL'

    Returns:
        model: Trained RandomForestClassifier model
        X: Features dataframe
        data: Original stock dataframe with indicators
    """

    # Fetch historical stock data (default daily data)
    data = yf.download(ticker, start="2020-01-01", auto_adjust=False)

    # -------------------------------
    # Feature Engineering: Technical Indicators
    # -------------------------------

    # 5-day Simple Moving Average (SMA)
    data['SMA5'] = data['Close'].rolling(5).mean()

    # 10-day Simple Moving Average
    data['SMA10'] = data['Close'].rolling(10).mean()

    # Relative Strength Index (RSI) calculation
    # RSI measures the speed and change of price movements
    data['RSI'] = 100 - (100 / (1 + (data['Close'].diff().clip(lower=0).rolling(14).mean() /
                                     data['Close'].diff().clip(upper=0).abs().rolling(14).mean())))

    # Drop rows with missing values due to rolling windows
    data = data.dropna()

    # -------------------------------
    # Target: 1 if next day's close > today's close, else 0
    # -------------------------------
    data['Target'] = (data['Close'].shift(-1) > data['Close']).astype(int)

    # Features and target
    X = data[['SMA5', 'SMA10', 'RSI']]  # Features
    y = data['Target']  # Labels

    # Split dataset into training and test sets (80% train, 20% test)
    # shuffle=False preserves the time order of stock data
    X_train, X_test, y_train, y_test = train_test_split(X, y, shuffle=False, test_size=0.2)

    # -------------------------------
    # Train ML model
    # -------------------------------
    model = RandomForestClassifier()  # Random Forest Classifier
    model.fit(X_train, y_train)  # Train on training data

    # Evaluate model accuracy on the test set
    print("ML Accuracy:", accuracy_score(y_test, model.predict(X_test)))

    return model, X, data


# -------------------------------
# 2. Function to Backtest (Simulate Fake Money Trading)
# -------------------------------
def backtest(model, X, data, initial_cash=10000):
    """
    Simulates trading using the ML model predictions.

    Args:
        model: Trained ML model
        X: Features dataframe
        data: Original stock dataframe (with Close prices)
        initial_cash (float): Starting capital

    Returns:
        final_value (float): Total portfolio value at the end
    """

    cash = initial_cash  # Start with $10,000
    shares = 0.0  # Start with 0 shares

    import numpy as np
    # Use the ML model to predict UP (1) or DOWN (0) for each day
    preds = model.predict(np.array(X))

    # Loop through each day in the dataset
    for i in range(len(X) - 1):
        price = float(data['Close'].iloc[i].item())  # Current day's closing price

        pred = int(preds[i]) #converting prediction into integer

        # If model predicts price will go UP → buy with all available cash
        if pred == 1 and cash > 0:
            buy_amount = 0.5*cash
            shares += buy_amount / price
            cash -= buy_amount

        # If model predicts price will go DOWN → sell all shares
        elif pred == 0 and shares > 0:
            sell_shares = 0.5*shares
            cash += sell_shares * price
            shares = -sell_shares

    # Final portfolio value: cash + value of any remaining shares
    final_value = cash + shares * float(data['Close'].iloc[-1].item())
    return final_value


# -------------------------------
# 3. Main workflow
# -------------------------------
if __name__ == "__main__":
    ticker = "NVDA"  # You can change this to any stock
    model, X, data = train_model(ticker)  # Train the ML model
    final_portfolio = backtest(model, X, data)  # Run the fake money backtest
    print(f"Final portfolio value for {ticker}: ${final_portfolio:.2f}")


