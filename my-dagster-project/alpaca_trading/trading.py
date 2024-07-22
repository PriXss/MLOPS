import os
import alpaca_trade_api as tradeapi

class AlpacaTrader:
    def __init__(self, api_key, api_secret, base_url, threshold):
        self.api = tradeapi.REST(api_key, api_secret, base_url, api_version='v2')
        self.threshold = threshold

    def get_account_info(self):
        account = self.api.get_account()
        return {
            "cash": float(account.cash),
            "buying_power": float(account.buying_power),
            "equity": float(account.equity),
            "last_equity": float(account.last_equity)
        }

    def get_latest_close(self, ticker):
        barset = self.api.get_barset(ticker, 'day', limit=1)
        bars = barset[ticker]
        return bars[0].c if bars else None

    def get_prediction(self, ticker):
        return float(os.getenv(f'PREDICTION_{ticker}'))

    def place_buy_order(self, ticker, qty):
        buy_order = self.api.submit_order(
            symbol=ticker,
            qty=qty,
            side='buy',
            type='market',
            time_in_force='gtc'
        )
        return buy_order

    def place_sell_order(self, ticker, qty):
        sell_order = self.api.submit_order(
            symbol=ticker,
            qty=qty,
            side='sell',
            type='market',
            time_in_force='gtc'
        )
        return sell_order

    def check_position(self, ticker):
        positions = self.api.list_positions()
        for position in positions:
            if position.symbol == ticker:
                return float(position.qty)
        return 0

    def execute_trade(self, ticker):
        account_info = self.get_account_info()
        latest_close = self.get_latest_close(ticker)
        predicted_close = self.get_prediction(ticker)

        if latest_close is None or predicted_close is None:
            print("Error: Could not retrieve necessary price information.")
            return

        price_difference = (predicted_close - latest_close) / latest_close

        if price_difference > self.threshold:
            max_shares = int(account_info['cash'] // latest_close)
            if max_shares > 0:
                buy_order = self.place_buy_order(ticker, max_shares)
                print("Buy Order:", buy_order)
            else:
                print("Not enough cash to buy shares.")
        elif price_difference < -self.threshold:
            position_qty = self.check_position(ticker)
            if position_qty > 0:
                sell_order = self.place_sell_order(ticker, position_qty)
                print("Sell Order:", sell_order)
            else:
                print(f"No shares of {ticker} to sell.")
        else:
            print("Price change within threshold, no action taken.")


if __name__ == "__main__":
    # Alpaca API Keys
    API_KEY = 'your_api_key'
    API_SECRET = 'your_api_secret'
    BASE_URL = 'https://paper-api.alpaca.markets'
    threshold = 0.005  # 0,5% Grenzwert

    trader = AlpacaTrader(API_KEY, API_SECRET, BASE_URL, threshold)
    trader.execute_trade('AAPL')