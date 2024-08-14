Alpaca Trader - Handelsautomatisierung
Übersicht
Der Alpaca Trader ist ein Python-Skript, das automatisierte Handelsentscheidungen auf Basis von Vorhersagemodellen trifft. Das Skript unterstützt sowohl Regressions- als auch Klassifikationsmodelle, um Kauf- und Verkaufsentscheidungen für Aktien zu treffen. Es verwendet die Alpaca API für den Zugriff auf Kontoinformationen und für die Durchführung von Handelsaufträgen.

Hauptklassen und Methoden
AlpacaTrader-Klasse
Die AlpacaTrader-Klasse verwaltet die Interaktion mit der Alpaca API und führt die Handelslogik basierend auf den Vorhersagen durch.

Konstruktor (__init__)
Parameter:

api_key, api_secret, base_url: API-Zugangsdaten für Alpaca.
threshold: Schwellenwert für Regressionsvorhersagen, um Kauf- oder Verkaufsentscheidungen zu treffen.
stocks: Ein Dictionary, das für jede Aktie die Vorhersage enthält.
context: Ein Logging-Kontext für detaillierte Protokolle.
prediction_type: Bestimmt, ob Regressions- oder Klassifikationsvorhersagen verwendet werden ('regression' oder 'classification').
Funktionalität:

Initialisiert die Verbindung zur Alpaca API und speichert die Vorhersagen und andere Parameter.
get_account_info
Funktion:
Ruft Kontoinformationen wie Bargeld, Kaufkraft und Eigenkapital ab.
Protokolliert die Kontoinformationen für Debugging und Überwachung.
get_latest_close
Parameter:

ticker: Das Aktiensymbol, dessen Schlusskurs abgerufen werden soll.
Funktion:

Ruft den letzten Schlusskurs der angegebenen Aktie ab und protokolliert den Wert.
get_prediction
Parameter:

ticker: Das Aktiensymbol, für das die Vorhersage abgerufen werden soll.
Funktion:

Holt die Vorhersage für die Aktie basierend auf dem Vorhersagetyp (Regression oder Klassifikation) und protokolliert den Wert.
place_buy_order
Parameter:

ticker: Das Aktiensymbol, für das die Kauforder platziert werden soll.
qty: Die Menge der zu kaufenden Aktien.
Funktion:

Platziert eine Kauforder über die Alpaca API und protokolliert die Details der Order.
place_sell_order
Parameter:

ticker: Das Aktiensymbol, für das die Verkaufsorder platziert werden soll.
qty: Die Menge der zu verkaufenden Aktien.
Funktion:

Platziert eine Verkaufsorder über die Alpaca API und protokolliert die Details der Order.
check_position
Parameter:

ticker: Das Aktiensymbol, für das die Position überprüft werden soll.
Funktion:

Überprüft die Menge der gehaltenen Aktien für das angegebene Symbol im Portfolio und protokolliert den Wert.
calculate_potential_gains_and_losses
Funktion:
Berechnet die potenziellen Gewinne und Verluste für alle Aktien im Dictionary auf Basis der Regressionsvorhersagen.
Gibt Dictionaries für mögliche Gewinne und Verluste zurück und protokolliert die berechneten Werte.
determine_best_and_worst
Parameter:

potential_gains: Dictionary mit möglichen Gewinnen für die Aktien.
potential_losses: Dictionary mit möglichen Verlusten für die Aktien.
Funktion:

Bestimmt die Aktie mit dem höchsten Gewinnpotenzial und die Aktie mit dem höchsten Verlustpotenzial.
Protokolliert die besten und schlechtesten Ticker.
sell_worst_loss_stock
Parameter:

worst_loss_ticker: Das Aktiensymbol mit dem höchsten Verlust.
positions: Dictionary der aktuellen Positionen im Portfolio.
Funktion:

Verkauft die Aktie mit dem höchsten Verlust, wenn sie im Portfolio vorhanden ist.
Protokolliert die Verkaufsdetails und den Erfolg der Order.
buy_best_gain_stock
Parameter:

best_gain_ticker: Das Aktiensymbol mit dem höchsten Gewinnpotenzial.
account_info: Dictionary mit Kontoinformationen.
Funktion:

Kauft die Aktie mit dem höchsten Gewinnpotenzial, wenn genügend Bargeld vorhanden ist.
Protokolliert die Kaufdetails und überprüft, ob genug Bargeld für den Kauf verfügbar ist.
execute_trade
Funktion:
Führt die Handelslogik basierend auf dem Vorhersagetyp ('regression' oder 'classification') aus.
Bei Regressionsvorhersagen:
Berechnet potenzielle Gewinne und Verluste.
Bestimmt die beste und schlechteste Aktie.
Verkauft die Aktie mit dem höchsten Verlust und kauft die Aktie mit dem höchsten Gewinn.
Bei Klassifikationsvorhersagen:
Führt Aktionen basierend auf den Empfehlungen aus ("kaufen", "verkaufen", "halten").
Kauft Aktien, die zum Kauf empfohlen werden, verkauft Aktien, die zum Verkauf empfohlen werden, und hält Aktien, die empfohlen werden, zu behalten.
Logik und Ablauf
Regressionsvorhersagen
Vorhersage und Positionen Abrufen:

Die Methode get_prediction wird verwendet, um den vorhergesagten Schlusskurs für jede Aktie zu erhalten.
Die Methode get_latest_close gibt den aktuellen Schlusskurs zurück.
Gewinne und Verluste Berechnen:

calculate_potential_gains_and_losses berechnet den Gewinn oder Verlust, den jede Aktie bringen könnte, und speichert diese Informationen in zwei Dictionaries: potential_gains und potential_losses.
Bestimmen der besten und schlechtesten Aktien:

determine_best_and_worst identifiziert die Aktie mit dem höchsten Gewinnpotenzial und die Aktie mit dem höchsten Verlustpotenzial.
Handelsentscheidungen Treffen:

sell_worst_loss_stock verkauft die Aktie mit dem höchsten Verlust, wenn diese im Portfolio ist.
buy_best_gain_stock kauft die Aktie mit dem höchsten Gewinnpotenzial, wenn genügend Bargeld vorhanden ist.
Klassifikationsvorhersagen
Vorhersage Abrufen:

Die Methode get_prediction gibt Empfehlungen wie "kaufen", "verkaufen" oder "halten" zurück.
Aktionen Ausführen:

Für "kaufen" wird überprüft, ob die Aktie bereits im Portfolio ist. Wenn nicht, wird überprüft, ob genug Bargeld vorhanden ist, und eine Kauforder wird platziert.
Für "verkaufen" wird überprüft, ob die Aktie im Portfolio ist. Wenn ja, wird eine Verkaufsorder platziert.
Für "halten" wird keine Aktion durchgeführt.