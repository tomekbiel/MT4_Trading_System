# scripts/send_heartbeat.py
import time
from mt4_connector import MT4CommandSender

def main():
    connector = MT4CommandSender(client_id="send_heartbeat", verbose=True)
    time.sleep(1)
    connector.send_heartbeat()
    time.sleep(2)
    connector.shutdown()

if __name__ == "__main__":
    main()
