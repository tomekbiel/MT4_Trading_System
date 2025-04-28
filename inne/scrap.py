# -*- coding: utf-8 -*-
"""
if __name__ == "__main__":
    print("Pakiet MT4 Connector został załadowany jako moduł.")
"""
"""
Główna klasa bazowa odpowiedzialna za połączenie z MT4.
Inicjalizuje podstawowe parametry, połączenia ZMQ i zarządza wspólnymi funkcjonalnościami.
"""

import zmq
import os
import time
from threading import Thread
from zmq.utils.monitor import recv_monitor_message
from datetime import datetime, timedelta
from pandas import Timestamp


class MT4BaseConnector:
    def __init__(self, client_id='tomasz_biel', host='localhost', protocol='tcp',
                 push_port=5555, pull_port=5556, sub_port=5557, verbose=True,
                 poll_timeout=1000, sleep_delay=0.001, monitor=False):
        """
        Inicjalizacja podstawowego połączenia z MT4.

        :param client_id: Unikalny identyfikator klienta (zachowane oryginalne 'tomasz_biel' jako domyślne)
        :param host: Adres hosta MT4
        :param protocol: Protokół komunikacji (tcp/ipc)
        :param push_port: Port do wysyłania komend
        :param pull_port: Port do odbierania odpowiedzi
        :param sub_port: Port do subskrypcji danych
        :param verbose: Czy wyświetlać szczegółowe logi
        :param poll_timeout: Timeout dla pollera w ms
        :param sleep_delay: Opóźnienie między sprawdzaniem wiadomości
        :param monitor: Czy monitorować sockety
        """
        self.active = True
        self.client_id = client_id  # Zachowane oryginalne nazewnictwo
        self.host = host
        self.protocol = protocol
        self.verbose = verbose
        self.poll_timeout = poll_timeout
        self.sleep_delay = sleep_delay

        # Porty
        self.push_port = push_port
        self.pull_port = pull_port
        self.sub_port = sub_port

        # Inicjalizacja kontekstu ZMQ
        self.zmq_context = zmq.Context()
        self.connection_url = f"{protocol}://{host}:"

        # Statusy socketów (zachowane oryginalne nazewnictwo)
        self.push_sock_status = {'state': True, 'latest_event': 'N/A'}
        self.pull_sock_status = {'state': True, 'latest_event': 'N/A'}

        # Główne sockety
        self._init_sockets()

        # Lista symboli - może być nadpisana przez dzieci (zachowana oryginalna lista)
        self.symbols = [
            'US.100+', 'EURUSD+', 'OIL.WTI+', 'OILs+', 'GOLDs+',
            'US.500+', 'US.30+', 'DE.30+',
            'JAP225+', 'UK.100+',
            'VIX+', 'W.20+', 'EURPLN+', 'USDJPY+'
        ]

        # Wątki
        self.data_thread = None
        self.push_monitor_thread = None
        self.pull_monitor_thread = None
        self._start_data_thread()

        # Monitorowanie socketów (zachowane oryginalne funkcjonalności)
        if monitor:
            self._init_socket_monitoring()

    def _init_sockets(self):
        """Inicjalizacja socketów ZMQ (zachowane oryginalne ustawienia)"""
        # Socket PUSH do wysyłania komend
        self.push_sock = self.zmq_context.socket(zmq.PUSH)
        self.push_sock.setsockopt(zmq.SNDHWM, 1)
        self.push_sock.connect(self.connection_url + str(self.push_port))

        # Socket PULL do odbierania odpowiedzi
        self.pull_sock = self.zmq_context.socket(zmq.PULL)
        self.pull_sock.setsockopt(zmq.RCVHWM, 1)
        self.pull_sock.connect(self.connection_url + str(self.pull_port))

        # Socket SUB do subskrypcji danych
        self.sub_sock = self.zmq_context.socket(zmq.SUB)
        self.sub_sock.connect(self.connection_url + str(self.sub_port))

        # Inicjalizacja pollera
        self.poller = zmq.Poller()
        self.poller.register(self.pull_sock, zmq.POLLIN)
        self.poller.register(self.sub_sock, zmq.POLLIN)

    def _init_socket_monitoring(self):
        """Inicjalizacja monitorowania socketów (zachowane oryginalne funkcjonalności)"""
        self.event_map = {}
        for name in dir(zmq):
            if name.startswith('EVENT_'):
                value = getattr(zmq, name)
                self.event_map[value] = name

        self.push_sock_status['state'] = False
        self.pull_sock_status['state'] = False

        self.push_monitor_thread = Thread(
            target=self._monitor_events,
            args=("PUSH", self.push_sock.get_monitor_socket())
        )
        self.push_monitor_thread.daemon = True
        self.push_monitor_thread.start()

        self.pull_monitor_thread = Thread(
            target=self._monitor_events,
            args=("PULL", self.pull_sock.get_monitor_socket())
        )
        self.pull_monitor_thread.daemon = True
        self.pull_monitor_thread.start()

    def _monitor_events(self, socket_name, monitor_socket):
        """Monitorowanie zdarzeń socketów (zachowane oryginalne funkcjonalności)"""
        while self.active:
            time.sleep(self.sleep_delay)
            while monitor_socket.poll(self.poll_timeout):
                try:
                    evt = recv_monitor_message(monitor_socket, zmq.DONTWAIT)
                    evt['description'] = self.event_map.get(evt['event'], 'UNKNOWN')

                    if self.verbose:
                        print(f"\n[{socket_name}] {evt['description']}")

                    if evt['event'] == 4096:  # EVENT_HANDSHAKE_SUCCEEDED
                        status = getattr(self, f"{socket_name.lower()}_sock_status")
                        status.update({'state': True, 'latest_event': 'EVENT_HANDSHAKE_SUCCEEDED'})
                    else:
                        status = getattr(self, f"{socket_name.lower()}_sock_status")
                        status.update({'state': False, 'latest_event': evt['description']})

                    if evt['event'] == zmq.EVENT_MONITOR_STOPPED:
                        monitor_socket = getattr(self, f"{socket_name.lower()}_sock").get_monitor_socket()

                except Exception as e:
                    print(f"Monitor error: {type(e).__name__}, Args: {e.args}")

        monitor_socket.close()

    def _start_data_thread(self):
        """Uruchomienie wątku do odbierania danych"""
        self.data_thread = Thread(target=self._poll_data)
        self.data_thread.daemon = True
        self.data_thread.start()

    def _poll_data(self):
        """Główna pętla odbierająca dane (do nadpisania przez dzieci)"""
        raise NotImplementedError("This method should be implemented in child classes")

    def send(self, data):
        """Wysyłanie danych przez socket PUSH (zachowane oryginalne zachowanie)"""
        if self.push_sock_status['state']:
            try:
                self.push_sock.send_string(data, zmq.DONTWAIT)
            except zmq.error.Again:
                print("\nResource timeout.. please try again.")
                time.sleep(self.sleep_delay)
        else:
            print('\n[KERNEL] NO HANDSHAKE ON PUSH SOCKET.. Cannot SEND data')

    def receive(self, socket):
        """Odbieranie danych (zachowane oryginalne nazewnictwo)"""
        if self.pull_sock_status['state']:
            try:
                return socket.recv_string(zmq.DONTWAIT)
            except zmq.error.Again:
                print("\nResource timeout.. please try again.")
                time.sleep(self.sleep_delay)
        else:
            print('\r[KERNEL] NO HANDSHAKE ON PULL SOCKET.. Cannot READ data', end='', flush=True)
        return None

    def shutdown(self):
        """Bezpieczne zamknięcie połączenia (zachowane oryginalne funkcjonalności)"""
        self.active = False

        if self.data_thread:
            self.data_thread.join()
        if self.push_monitor_thread:
            self.push_monitor_thread.join()
        if self.pull_monitor_thread:
            self.pull_monitor_thread.join()

        self.poller.unregister(self.pull_sock)
        self.poller.unregister(self.sub_sock)
        if self.verbose:
            print("\n++ [KERNEL] Sockets unregistered from ZMQ Poller()! ++")

        self.zmq_context.destroy(0)
        if self.verbose:
            print("\n++ [KERNEL] ZeroMQ Context Terminated.. shut down safely complete! :)")

    def set_status(self, new_status=False):
        """Ustawienie statusu (zachowane oryginalne nazewnictwo)"""
        self.active = new_status
        if self.verbose:
            print(f"\n**\n[KERNEL] Setting Status to {new_status} - Deactivating Threads.. please wait a bit.\n**")





