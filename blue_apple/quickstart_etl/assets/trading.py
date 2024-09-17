import os
import alpaca_trade_api as tradeapi
import logging


class AlpacaTrader:
    def __init__(self, api_key, api_secret, base_url, threshold, stocks, context, prediction_type='regression'):
        """
        Initialisiert den AlpacaTrader mit API-Schlüsseln, Basis-URL, Schwellenwert und Aktieninformationen.
        """
        self.api = tradeapi.REST(api_key, api_secret, base_url, api_version='v2')
        self.threshold = threshold
        self.stocks = stocks  # Dictionary mit Aktienkürzeln und deren Vorhersagen
        self.context = context
        self.logger = context.log
        self.prediction_type = prediction_type  # 'regression' oder 'classification'
        self.logger.info("AlpacaTrader initialized")

    def get_account_info(self):
        """
        Ruft Kontoinformationen vom Alpaca-API ab und gibt diese zurück.
        """
        account = self.api.get_account()
        info = {
            "cash": float(account.cash),
            "buying_power": float(account.buying_power),
            "equity": float(account.equity),
            "last_equity": float(account.last_equity)
        }
        self.logger.info(f"Account Info: {info}")
        return info

    def get_latest_close(self, ticker):
        """
        Ruft den letzten Schlusskurs der angegebenen Aktie ab.
        """
        barset = self.api.get_barset(ticker, 'day', limit=1)
        bars = barset[ticker]
        latest_close = bars[0].c if bars else None
        self.logger.info(f"Latest close for {ticker}: {latest_close}")
        return latest_close

    def get_prediction(self, ticker):
        """
        Ruft die Vorhersage für die angegebene Aktie ab.
        Die Vorhersage hängt vom Modelltyp ab (Regression oder Klassifikation).
        """
        prediction = self.stocks.get(ticker)
        self.logger.info(f"Prediction for {ticker}: {prediction}")
        return prediction

    def place_buy_order(self, ticker, qty):
        """
        Platziert eine Kauforder für die angegebene Aktie.
        """
        self.logger.info(f"Placing buy order for {ticker}: {qty} shares")
        buy_order = self.api.submit_order(
            symbol=ticker,
            qty=qty,
            side='buy',
            type='market',
            time_in_force='gtc'
        )
        self.logger.info(f"Buy order placed for {ticker}: {buy_order}")
        return buy_order

    def place_sell_order(self, ticker, qty):
        """
        Platziert eine Verkaufsorder für die angegebene Aktie.
        """
        self.logger.info(f"Placing sell order for {ticker}: {qty} shares")
        sell_order = self.api.submit_order(
            symbol=ticker,
            qty=qty,
            side='sell',
            type='market',
            time_in_force='gtc'
        )
        self.logger.info(f"Sell order placed for {ticker}: {sell_order}")
        return sell_order

    def check_position(self, ticker):
        """
        Überprüft die Position der angegebenen Aktie im Portfolio.
        """
        positions = self.api.list_positions()
        for position in positions:
            if position.symbol == ticker:
                qty = float(position.qty)
                self.logger.info(f"Position for {ticker}: {qty} shares")
                return qty
        self.logger.info(f"No position found for {ticker}")
        return 0

    def calculate_potential_gains_and_losses(self):
        """
        Berechnet die potenziellen Gewinne und Verluste für die Aktien, wenn das Modell Regressionsvorhersagen liefert.
        """
        potential_gains = {}
        potential_losses = {}

        for ticker in self.stocks:
            latest_close = self.get_latest_close(ticker)
            predicted_close = self.get_prediction(ticker)

            if latest_close is None or predicted_close is None:
                self.logger.error(f"Error: Could not retrieve necessary price information for {ticker}.")
                continue

            price_difference = (predicted_close - latest_close) / latest_close
            self.logger.info(f"Price difference for {ticker}: {price_difference}")

            if price_difference > 0:
                potential_gains[ticker] = price_difference
            else:
                potential_losses[ticker] = price_difference

        self.logger.info(f"Potential gains: {potential_gains}")
        self.logger.info(f"Potential losses: {potential_losses}")

        return potential_gains, potential_losses

    def determine_best_and_worst(self, potential_gains, potential_losses):
        """
        Bestimmt die Aktie mit dem höchsten vorhergesagten Gewinn und die Aktie im Portfolio mit dem höchsten Verlust.
        """
        best_gain_ticker = max(potential_gains, key=potential_gains.get, default=None)
        worst_loss_ticker = min(potential_losses, key=potential_losses.get, default=None)

        self.logger.info(f"Best gain ticker: {best_gain_ticker}")
        self.logger.info(f"Worst loss ticker: {worst_loss_ticker}")

        return best_gain_ticker, worst_loss_ticker

    def sell_worst_loss_stock(self, worst_loss_ticker, positions):
        """
        Verkauft die Aktie im Portfolio mit dem höchsten Verlust.
        """
        if worst_loss_ticker and worst_loss_ticker in positions:
            qty = positions[worst_loss_ticker]
            sell_order = self.place_sell_order(worst_loss_ticker, qty)
            self.logger.info(f"Sell order executed for {worst_loss_ticker}: {sell_order}")
            return True
        return False

    def buy_best_gain_stock(self, best_gain_ticker, account_info):
        """
        Kauft die Aktie mit dem höchsten vorhergesagten Gewinn.
        """
        if best_gain_ticker:
            latest_close_best_gain = self.get_latest_close(best_gain_ticker)
            if latest_close_best_gain:
                max_shares = int(account_info['cash'] // latest_close_best_gain)
                self.logger.info(f"Maximum shares to buy for {best_gain_ticker}: {max_shares}")
                if max_shares > 0:
                    buy_order = self.place_buy_order(best_gain_ticker, max_shares)
                    self.logger.info(f"Buy order executed for {best_gain_ticker}: {buy_order}")
                else:
                    self.logger.warning(f"Not enough cash to buy shares of {best_gain_ticker}.")

    def execute_trade(self):
        """
        Führt die Handelslogik basierend auf Vorhersagen durch, die entweder durch Regression oder Klassifikation erstellt wurden.
        """
        self.logger.info("Executing trade")
        account_info = self.get_account_info()

        # Holen der aktuellen Positionen
        positions = {pos.symbol: float(pos.qty) for pos in self.api.list_positions()}
        self.logger.info(f"Current positions: {positions}")

        if self.prediction_type == 'regression':
            # Berechnung der potenziellen Gewinne und Verluste für Regression
            potential_gains, potential_losses = self.calculate_potential_gains_and_losses()
            best_gain_ticker, worst_loss_ticker = self.determine_best_and_worst(potential_gains, potential_losses)

            # Verkauf der Aktie mit dem höchsten Verlust
            if self.sell_worst_loss_stock(worst_loss_ticker, positions):
                # Kauf der Aktie mit dem höchsten Gewinn, wenn sie genug Cash haben
                self.buy_best_gain_stock(best_gain_ticker, account_info)
            else:
                # Wenn keine Aktien verkauft wurden und es eine beste Gewinnaktie gibt
                if best_gain_ticker and best_gain_ticker not in positions:
                    self.buy_best_gain_stock(best_gain_ticker, account_info)

        elif self.prediction_type == 'classification':
            self.logger.info("Processing classification predictions")
            for ticker in self.stocks:
                prediction = self.get_prediction(ticker)

                if prediction == 'buy':
                    # Wenn das Modell empfiehlt, die Aktie zu kaufen
                    if ticker not in positions:
                        # Wenn die Aktie nicht im Portfolio ist
                        latest_close = self.get_latest_close(ticker)
                        if latest_close:
                            max_shares = int(account_info['cash'] // latest_close)
                            self.logger.info(f"Maximum shares to buy for {ticker}: {max_shares}")
                            if max_shares > 0:
                                self.place_buy_order(ticker, max_shares)
                            else:
                                self.logger.warning(f"Not enough cash to buy shares of {ticker}.")
                    else:
                        self.logger.info(f"Already holding {ticker}, no action needed.")

                elif prediction == 'sell':
                    # Wenn das Modell empfiehlt, die Aktie zu verkaufen
                    if ticker in positions:
                        qty = positions[ticker]
                        self.place_sell_order(ticker, qty)

                elif prediction == 'hold':
                    # Wenn das Modell empfiehlt, die Aktie zu halten
                    self.logger.info(f"Holding recommendation for {ticker}, no action needed.")
