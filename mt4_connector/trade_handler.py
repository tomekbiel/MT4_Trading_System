# -*- coding: utf-8 -*-
"""
Handler operacji handlowych - wersja zintegrowana z poprawioną klasą bazową
"""

from .base_connector import MT4BaseConnector
import zmq
import time


class MT4TradeHandler(MT4BaseConnector):
    def __init__(self, **kwargs):
        """
        Handler do obsługi operacji handlowych.
        """
        super().__init__(**kwargs)
        self.order_template = self.default_order()  # Zachowane oryginalne nazewnictwo
        self.open_trades = {}
        self.account_info = {}  # Dodane dla kompatybilności z oryginałem

    def default_order(self):
        """Domyślne parametry zlecenia (zachowane oryginalne nazewnictwo)"""
        return {
            '_action': 'OPEN',
            '_type': 0,  # 0 = BUY, 1 = SELL
            '_symbol': 'US.100+',
            '_price': 0.0,
            '_SL': 500,
            '_TP': 500,
            '_comment': self.client_id,
            '_lots': 0.01,
            '_magic': 123456,
            '_ticket': 0
        }

    def new_trade(self, order=None):
        """Nowe zlecenie (zachowane oryginalne nazewnictwo)"""
        if order is None:
            order = self.default_order()
        self.send_command(**order)  # Zachowana oryginalna nazwa metody

    def modify_trade(self, _ticket, _SL, _TP, _price=0):
        """Modyfikacja zlecenia (zachowane oryginalne nazewnictwo parametrów)"""
        try:
            self.order_template.update({
                '_action': 'MODIFY',
                '_ticket': _ticket,
                '_SL': _SL,
                '_TP': _TP,
                '_price': _price
            })
            self.send_command(**self.order_template)
        except KeyError:
            print(f"[ERROR] Order Ticket {_ticket} not found!")

    def close_trade(self, _ticket):
        """Zamknięcie zlecenia (zachowane oryginalne nazewnictwo)"""
        try:
            self.order_template.update({
                '_action': 'CLOSE',
                '_ticket': _ticket
            })
            self.send_command(**self.order_template)
        except KeyError:
            print(f"[ERROR] Order Ticket {_ticket} not found!")

    def close_partial(self, _ticket, _lots):
        """Zamknięcie częściowe zlecenia (zachowane z oryginału)"""
        try:
            self.order_template.update({
                '_action': 'CLOSE_PARTIAL',
                '_ticket': _ticket,
                '_lots': _lots
            })
            self.send_command(**self.order_template)
        except KeyError:
            print(f"[ERROR] Order Ticket {_ticket} not found!")

    def close_by_magic(self, _magic):
        """Zamknięcie zleceń po magic number (zachowane z oryginału)"""
        try:
            self.order_template.update({
                '_action': 'CLOSE_MAGIC',
                '_magic': _magic
            })
            self.send_command(**self.order_template)
        except KeyError:
            pass

    def close_all(self):
        """Zamknięcie wszystkich zleceń (zachowane z oryginału)"""
        try:
            self.order_template.update({'_action': 'CLOSE_ALL'})
            self.send_command(**self.order_template)
        except KeyError:
            pass

    def get_open_trades(self):
        """Pobranie otwartych zleceń (zachowane oryginalne nazewnictwo)"""
        try:
            self.order_template.update({'_action': 'get_open_trades'})
            self.send_command(**self.order_template)
        except KeyError:
            pass

    def send_command(self, _action='OPEN', _type=0, _symbol='US.100+', _price=0.0,
                     _SL=50, _TP=50, _comment=None, _lots=0.01,
                     _magic=123456, _ticket=0):
        """Wysłanie komendy handlowej (zachowane oryginalne nazewnictwo)"""
        if _comment is None:
            _comment = self.client_id

        msg = f"TRADE;{_action};{_type};{_symbol};{_price};{_SL};{_TP};{_comment};{_lots};{_magic};{_ticket}"
        self.send(msg)

    def get_account_info(self):
        """Pobranie informacji o koncie (zachowane z oryginału)"""
        try:
            self.order_template.update({'_action': 'GET_ACCOUNT_INFO'})
            self.send_command(**self.order_template)
        except Exception as e:
            print(f"Error getting account info: {type(e).__name__}, Args: {e.args}")

    def _poll_data(self):
        """Główna pętla odbierająca dane handlowe (zachowane oryginalne funkcjonalności)"""
        while self.active:
            time.sleep(self.sleep_delay)
            sockets = dict(self.poller.poll(self.poll_timeout))

            if self.pull_sock in sockets and sockets[self.pull_sock] == zmq.POLLIN:
                msg = self.receive(self.pull_sock)
                if msg:
                    try:
                        data = eval(msg)
                        if '_action' in data:
                            if data['_action'] == 'get_open_trades':
                                self.open_trades = data.get('_data', {})
                                if self.verbose:
                                    print("Open trades updated:", self.open_trades)

                            elif data['_action'] == 'GET_ACCOUNT_INFORMATION':
                                acc_num = data['account_number']
                                if '_data' in data:
                                    self.account_info.setdefault(acc_num, []).extend(data['_data'])
                                    if self.verbose:
                                        print("Account info updated:", self.account_info)

                        self.last_response = data  # Zachowane z oryginału
                        if self.verbose:
                            print(data)

                    except Exception as e:
                        print(f"Exception: {type(e).__name__}, Args: {e.args}")

    def send_heartbeat(self):
        """Wysłanie heartbeat (zachowane z oryginału)"""
        self.send(self.push_sock, "HEARTBEAT;")

"""
# Inicjalizacja
trade_handler = MT4TradeHandler(
    client_id='tomasz_biel',
    verbose=True
)

# Otwarcie nowego zlecenia
trade_handler.new_trade({
    '_symbol': 'EURUSD+',
    '_type': 0,  # BUY
    '_lots': 0.1,
    '_SL': 50,
    '_TP': 50
})

# Pobranie otwartych zleceń
trade_handler.get_open_trades()
time.sleep(1)  # Czekamy na odpowiedź
print("Otwarte zlecenia:", trade_handler.open_trades)

# Zamknięcie wszystkich zleceń
# trade_handler.close_all()

# Po zakończeniu
# trade_handler.shutdown()
"""