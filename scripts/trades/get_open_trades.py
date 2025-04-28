import os
import time
import json
from mt4_connector import MT4CommandSender


class GetOpenTrades(MT4CommandSender):
    def _process_message(self, msg):
        try:
            msg_fixed = msg.replace("'", '"')
            msg_fixed = msg_fixed.replace("False", "false").replace("True", "true")
            parsed = json.loads(msg_fixed)

            if "_action" in parsed and parsed["_action"] == "OPEN_TRADES":
                print("✅ Otwartych transakcji:", len(parsed["_trades"]))
                for ticket, trade in parsed["_trades"].items():
                    print(
                        f"🎯 Ticket: {ticket}, Symbol: {trade['_symbol']}, Lots: {trade['_lots']}, Type: {trade['_type']}")
                self.save_trades_to_csv(parsed["_trades"])
            else:
                print("⚠️ Odpowiedź nie zawiera otwartych transakcji.")

        except Exception as e:
            print(f"❌ Błąd przy parsowaniu wiadomości: {e}")

    def _process_stream_message(self, msg):
        pass

    def save_trades_to_csv(self, trades):
        now = time.strftime("%Y-%m-%d_%H-%M-%S")
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        directory = os.path.join(base_dir, "data", "trades")
        os.makedirs(directory, exist_ok=True)

        filepath = os.path.join(directory, f"open_trades_{now}.csv")

        with open(filepath, "w", encoding="utf-8") as f:
            f.write("ticket,symbol,lots,type,open_price,open_time,SL,TP,pnl,comment\n")
            for ticket, trade in trades.items():
                f.write(
                    f"{ticket},{trade.get('_symbol', '')},{trade.get('_lots', '')},{trade.get('_type', '')},"
                    f"{trade.get('_open_price', '')},{trade.get('_open_time', '')},{trade.get('_SL', '')},{trade.get('_TP', '')},"
                    f"{trade.get('_pnl', '')},{trade.get('_comment', '')}\n"
                )

        print(f"💾 Dane zapisane do: {filepath}")


def main():
    print("📡 Wysyłam komendę → TRADE;GET_OPEN_TRADES")

    connector = GetOpenTrades(client_id="get_open_trades", verbose=True)
    time.sleep(1)

    command = "TRADE;GET_OPEN_TRADES"
    success = connector.send(command)

    if not success:
        print("❌ Nie udało się wysłać komendy.")
    else:
        time.sleep(5)

    connector.shutdown()


if __name__ == "__main__":
    main()
