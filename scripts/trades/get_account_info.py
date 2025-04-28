import os
import time
import json
from mt4_connector import MT4CommandSender


class GetAccountInfo(MT4CommandSender):
    def _process_message(self, msg):
        try:
            msg_fixed = msg.replace("'", '"')
            msg_fixed = msg_fixed.replace("False", "false").replace("True", "true")
            parsed = json.loads(msg_fixed)

            if "_action" in parsed and parsed["_action"] == "GET_ACCOUNT_INFORMATION":
                print("✅ Informacje o koncie:")
                for info in parsed.get("_data", []):
                    print(f"📋 {info}")
                self.save_account_info_to_csv(parsed)
            else:
                print("⚠️ Odpowiedź nie zawiera danych konta.")

        except Exception as e:
            print(f"❌ Błąd przy parsowaniu wiadomości: {e}")

    def _process_stream_message(self, msg):
        pass

    def save_account_info_to_csv(self, account_info):
        now = time.strftime("%Y-%m-%d_%H-%M-%S")
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        directory = os.path.join(base_dir, "data", "trades")
        os.makedirs(directory, exist_ok=True)

        account_number = account_info.get("account_number", "unknown")
        filepath = os.path.join(directory, f"account_info_{account_number}_{now}.csv")

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(
                "currenttime,account_name,account_balance,account_equity,account_profit,account_free_margin,account_leverage\n")
            for item in account_info.get("_data", []):
                f.write(
                    f"{item.get('currenttime', '')},{item.get('account_name', '')},{item.get('account_balance', '')},"
                    f"{item.get('account_equity', '')},{item.get('account_profit', '')},{item.get('account_free_margin', '')},"
                    f"{item.get('account_leverage', '')}\n"
                )

        print(f"💾 Dane konta zapisane do: {filepath}")


def main():
    print("📡 Wysyłam komendę → TRADE;GET_ACCOUNT_INFO")

    connector = GetAccountInfo(client_id="get_account_info", verbose=True)
    time.sleep(1)

    command = "TRADE;GET_ACCOUNT_INFO"
    success = connector.send(command)

    if not success:
        print("❌ Nie udało się wysłać komendy.")
    else:
        time.sleep(5)

    connector.shutdown()


if __name__ == "__main__":
    main()
