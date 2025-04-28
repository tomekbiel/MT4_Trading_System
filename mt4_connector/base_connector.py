# -*- coding: utf-8 -*-
"""
MT4 Base Connector - Core connection handler with socket monitoring
"""

import zmq
import time
import logging
import json
import os
from threading import Thread, Event
from zmq.utils.monitor import recv_monitor_message


class MT4BaseConnector:
    """
    Enhanced MT4 connection handler with:
    - Automatic socket monitoring
    - Connection recovery
    - Thread-safe operations
    """

    # Socket event mappings
    SOCKET_EVENTS = {
        zmq.EVENT_CONNECTED: "CONNECTED",
        zmq.EVENT_CONNECT_DELAYED: "CONNECT_DELAYED",
        zmq.EVENT_CONNECT_RETRIED: "CONNECT_RETRIED",
        zmq.EVENT_LISTENING: "LISTENING",
        zmq.EVENT_BIND_FAILED: "BIND_FAILED",
        zmq.EVENT_ACCEPTED: "ACCEPTED",
        zmq.EVENT_ACCEPT_FAILED: "ACCEPT_FAILED",
        zmq.EVENT_CLOSED: "CLOSED",
        zmq.EVENT_CLOSE_FAILED: "CLOSE_FAILED",
        zmq.EVENT_DISCONNECTED: "DISCONNECTED",
        zmq.EVENT_MONITOR_STOPPED: "MONITOR_STOPPED"
    }

    def __init__(self, client_id=None, config=None, **kwargs):
        from . import DEFAULT_CONFIG

        self.config = DEFAULT_CONFIG.copy()
        if config:
            self.config.update(config)
        self.config.update(kwargs)

        self.poll_timeout = self.config['NETWORK'].get('timeout', 1000)
        self.retry_delay = self.config['NETWORK'].get('retry_delay', 1.0)
        self.max_retries = self.config['NETWORK'].get('retries', 3)
        self.host = self.config['NETWORK'].get('host', 'localhost')
        self.protocol = self.config['NETWORK'].get('protocol', 'tcp')

        # 🔄 Zamiana portów zgodnie z MQL4 DWX Server: MT4 PULL <-- Python PUSH, MT4 PUSH --> Python PULL
        self.push_port = self.config['PORTS'].get('pull', 5556)  # MT4: PULL  <-- Python: PUSH
        self.pull_port = self.config['PORTS'].get('push', 5555)  # MT4: PUSH  --> Python: PULL
        self.sub_port = self.config['PORTS'].get('sub', 5557)

        self.verbose = self.config.get('verbose', True)
        self.sleep_delay = self.config['NETWORK'].get('sleep_delay', 0.1)

        self.logger = logging.getLogger(__name__)
        self.client_id = client_id or f"client_{time.time()}"
        self.active = True
        self._connection_ready = Event()
        self._shutdown_initiated = False
        self._resources = {
            'threads': [],
            'sockets': [],
            'monitors': []
        }

        self._socket_status = {
            'push': {'connected': False, 'last_event': None},
            'pull': {'connected': False, 'last_event': None},
            'sub': {'connected': False, 'last_event': None}
        }

        self.symbols = []
        self.timeframes = {}
        self._load_symbols_and_timeframes()

        self._initialize_connection()


    def _initialize_connection(self):
        """Establish connection with retry mechanism"""
        retry_count = 0
        last_error = None

        while retry_count <= self.config['NETWORK']['retries'] and not self._shutdown_initiated:
            try:
                self.context = zmq.Context()
                self._init_sockets()
                self._start_data_thread()
                self._init_socket_monitoring()

                self._connection_ready.set()
                self.logger.info(f"Connection established for {self.client_id}")
                return

            except zmq.ZMQError as e:
                retry_count += 1
                last_error = e
                self.logger.error(f"Connection failed (attempt {retry_count}): {e}")
                if retry_count <= self.config['NETWORK']['retries']:
                    time.sleep(self.config['NETWORK']['retry_delay'])
                self._cleanup_resources()

        error_msg = f"Connection failed after {retry_count} attempts. Last error: {last_error}"
        self.logger.error(error_msg)
        raise ConnectionError(error_msg)

    def _init_sockets(self):
        """Initialize ZMQ sockets with current configuration"""
        try:
            # Initialize sockets
            self.push_sock = self.context.socket(zmq.PUSH)
            self.pull_sock = self.context.socket(zmq.PULL)
            self.sub_sock = self.context.socket(zmq.SUB)

            # Configure sockets
            for sock in [self.push_sock, self.pull_sock, self.sub_sock]:
                sock.setsockopt(zmq.LINGER, 0)

            # Establish connections
            self.push_sock.connect(
                f"{self.config['NETWORK']['protocol']}://"
                f"{self.config['NETWORK']['host']}:"
                f"{self.config['PORTS']['push']}"
            )

            self.pull_sock.connect(
                f"{self.config['NETWORK']['protocol']}://"
                f"{self.config['NETWORK']['host']}:"
                f"{self.config['PORTS']['pull']}"
            )

            self.sub_sock.connect(
                f"{self.config['NETWORK']['protocol']}://"
                f"{self.config['NETWORK']['host']}:"
                f"{self.config['PORTS']['sub']}"
            )
            self.sub_sock.setsockopt_string(zmq.SUBSCRIBE, '')

            # Setup poller
            self.poller = zmq.Poller()
            self.poller.register(self.pull_sock, zmq.POLLIN)
            self.poller.register(self.sub_sock, zmq.POLLIN)

        except zmq.ZMQError as e:
            self.logger.error(f"Socket initialization failed: {e}")
            self._cleanup_sockets()
            raise

    def _load_symbols_and_timeframes(self, config_path=None):
        try:
            # 🛠️ Ustal absolutną ścieżkę do katalogu projektu
            base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
            config_file = os.path.join(base_dir, 'config', 'symbols.json') if config_path is None else config_path

            with open(config_file, "r") as f:
                data = json.load(f)

            self.symbols = data.get("symbols", [])
            self.timeframes = data.get("timeframes", {})

            self.logger.info(f"Załadowano symbole: {len(self.symbols)}, ramy czasowe: {len(self.timeframes)}")

        except Exception as e:
            self.logger.warning(f"Błąd ładowania symboli/timeframes z JSON: {e}")
            self.symbols = ['US.100+', 'EURUSD+', 'GOLDs+']  # fallback
            self.timeframes = {"M15": {"max_days": 10}}  # fallback

    def get_max_history_range(self, timeframe):
        tf_data = self.timeframes.get(timeframe)
        if not tf_data:
            return "Nieznany timeframe"

        if "max_days" in tf_data:
            val = tf_data['max_days']
            return f"{val} dni" if isinstance(val, int) else val
        elif "max_months" in tf_data:
            val = tf_data['max_months']
            return f"{val} miesięcy" if isinstance(val, int) else val
        elif "max_years" in tf_data:
            val = tf_data['max_years']
            return f"{val} lat" if isinstance(val, int) else val
        else:
            return "Nieokreślony zakres"

    def _start_data_thread(self):
        """Uruchomienie wątku do przetwarzania danych."""
        self.data_thread = Thread(target=self._data_loop, name="DataThread", daemon=True)
        self.data_thread.start()
        self._resources['threads'].append(self.data_thread)
        self.logger.debug("Wątek danych uruchomiony")

    def _init_socket_monitoring(self):
        """Pełna implementacja monitorowania gniazd."""
        try:
            # Monitorowanie gniazda PUSH
            push_monitor = self.push_sock.get_monitor_socket()
            push_thread = Thread(target=self._monitor_loop, args=(push_monitor, 'push'),
                                 name="PushMonitorThread", daemon=True)
            push_thread.start()
            self._resources['monitors'].append(push_monitor)
            self._resources['threads'].append(push_thread)

            # Monitorowanie gniazda PULL
            pull_monitor = self.pull_sock.get_monitor_socket()
            pull_thread = Thread(target=self._monitor_loop, args=(pull_monitor, 'pull'),
                                 name="PullMonitorThread", daemon=True)
            pull_thread.start()
            self._resources['monitors'].append(pull_monitor)
            self._resources['threads'].append(pull_thread)

            # Monitorowanie gniazda SUB
            sub_monitor = self.sub_sock.get_monitor_socket()
            sub_thread = Thread(target=self._monitor_loop, args=(sub_monitor, 'sub'),
                                name="SubMonitorThread", daemon=True)
            sub_thread.start()
            self._resources['monitors'].append(sub_monitor)
            self._resources['threads'].append(sub_thread)

            self.logger.info("Monitoring gniazd został uruchomiony")

        except Exception as e:
            self.logger.error(f"Błąd inicjalizacji monitorowania gniazd: {str(e)}")
            raise

    def _monitor_loop(self, monitor_socket, socket_type):
        """Pętla monitorująca zdarzenia gniazda."""
        self.logger.debug(f"Rozpoczęto monitoring gniazda {socket_type}")
        while self.active and not self._shutdown_initiated:
            try:
                event = recv_monitor_message(monitor_socket, flags=zmq.NOBLOCK)
                if event:
                    event_name = self.SOCKET_EVENTS.get(event['event'], "UNKNOWN")
                    self.logger.debug(f"Zdarzenie gniazda {socket_type}: {event_name} (addr: {event['endpoint']})")

                    # Aktualizacja statusu gniazda
                    self._update_socket_status(socket_type, event['event'], event_name)

                    # Obsługa krytycznych zdarzeń
                    if event['event'] == zmq.EVENT_DISCONNECTED:
                        self.logger.warning(f"Rozłączono gniazdo {socket_type}! Próba ponownego połączenia...")
                        self._handle_disconnection(socket_type)

            except zmq.ZMQError as e:
                if e.errno != zmq.EAGAIN:
                    self.logger.error(f"Błąd monitorowania gniazda {socket_type}: {str(e)}")
                time.sleep(0.1)
            except Exception as e:
                self.logger.error(f"Nieoczekiwany błąd w pętli monitorującej {socket_type}: {str(e)}")
                time.sleep(1)

        self.logger.debug(f"Zakończono monitoring gniazda {socket_type}")

    def _update_socket_status(self, socket_type, event_code, event_name):
        """Aktualizacja statusu gniazda na podstawie zdarzenia."""
        if socket_type in self._socket_status:
            self._socket_status[socket_type]['last_event'] = {
                'code': event_code,
                'name': event_name,
                'timestamp': time.time()
            }

            # Aktualizacja flagi połączenia
            if event_code == zmq.EVENT_CONNECTED:
                self._socket_status[socket_type]['connected'] = True
            elif event_code in [zmq.EVENT_DISCONNECTED, zmq.EVENT_CLOSED]:
                self._socket_status[socket_type]['connected'] = False

    def _handle_disconnection(self, socket_type):
        """Obsługa rozłączenia gniazda."""
        if socket_type == 'push':
            self._reconnect_socket('push', self.push_sock, self.push_port)
        elif socket_type == 'pull':
            self._reconnect_socket('pull', self.pull_sock, self.pull_port)
        elif socket_type == 'sub':
            self._reconnect_socket('sub', self.sub_sock, self.sub_port)

    # 🛠️ POPRAWKA: Zmodyfikowana metoda _reconnect_socket z unregister starego socketu
    def _reconnect_socket(self, socket_type, socket, port):
        retry_count = 0
        while retry_count < self.max_retries and self.active and not self._shutdown_initiated:
            try:
                self.logger.info(f"🔁 Reconnect {socket_type} ({retry_count + 1}/{self.max_retries})")

                # ➕ DEBUG: stan pollera przed
                self.logger.debug(f"📋 Poller gniazda PRZED reconnect: {self.poller.sockets}")

                # 🧹 Wyrejestrowanie starego socketu z pollera – tylko jeśli istnieje
                if socket_type in ['pull', 'sub']:
                    try:
                        if socket in dict(self.poller.sockets):
                            self.logger.debug(f"🧽 Wyrejestrowuję stare gniazdo {socket_type} z pollera")
                            self.poller.unregister(socket)
                        else:
                            self.logger.debug(f"ℹ️ Gniazdo {socket_type} nie było zarejestrowane w pollerze")
                    except Exception as e:
                        self.logger.warning(f"❗ Błąd przy wyrejestrowywaniu {socket_type}: {e}")

                if not socket.closed:
                    socket.close()
                    self.logger.debug(f"✅ Zamknięto stare gniazdo {socket_type}")

                # 🔧 Nowe gniazdo
                new_socket = self.context.socket(
                    zmq.PUSH if socket_type == 'push' else
                    zmq.PULL if socket_type == 'pull' else zmq.SUB
                )
                new_socket.setsockopt(zmq.LINGER, 0)
                new_socket.connect(f"{self.protocol}://{self.host}:{port}")
                self.logger.debug(f"{socket_type.upper()} socket closed? {new_socket.closed}")

                if socket_type == 'sub':
                    new_socket.setsockopt_string(zmq.SUBSCRIBE, '')

                # 🔄 Aktualizacja referencji
                if socket_type == 'push':
                    self.push_sock = new_socket
                elif socket_type == 'pull':
                    self.pull_sock = new_socket
                else:
                    self.sub_sock = new_socket

                # 📬 Rejestracja nowego w pollerze
                if socket_type in ['pull', 'sub']:
                    try:
                        self.poller.register(new_socket, zmq.POLLIN)
                        self.logger.debug(f"✅ Zarejestrowano nowe gniazdo {socket_type} w pollerze: {new_socket}")
                    except Exception as e:
                        self.logger.error(f"❌ Błąd rejestracji nowego {socket_type} w pollerze: {e}")

                self.logger.debug(f"📋 Poller gniazda PO reconnect: {self.poller.sockets}")
                self.logger.info(f"✅ Ponowne połączenie gniazda {socket_type} powiodło się")
                return True

            except zmq.ZMQError as e:
                retry_count += 1
                self.logger.error(f"❌ Błąd reconnect {socket_type}: {str(e)}")
                if retry_count < self.max_retries:
                    time.sleep(self.retry_delay)

        self.logger.error(f"🛑 Nie udało się połączyć {socket_type} po {retry_count} próbach")
        return False

    def send(self, message):
        """
        Wysyłanie wiadomości do MT4 z rozszerzoną obsługą błędów.

        :param message: Wiadomość do wysłania (string)
        :return: True jeśli wysłano pomyślnie, False w przeciwnym razie
        """
        if not self.is_push_connected:
            self.logger.warning("Próba wysłania wiadomości przez niepołączone gniazdo PUSH")
            return False

        retry_count = 0
        while retry_count <= self.max_retries and self.active:
            try:
                self.push_sock.send_string(message, zmq.DONTWAIT)
                self.logger.debug(f"Wysłano wiadomość: {message}")
                return True

            except zmq.ZMQError as e:
                retry_count += 1
                self.logger.error(f"Błąd wysyłania (próba {retry_count}/{self.max_retries}): {str(e)}")

                if e.errno == zmq.EAGAIN:  # Tymczasowy błąd
                    time.sleep(self.retry_delay)
                else:  # Krytyczny błąd
                    self._handle_disconnection('push')
                    break

        self.logger.error(f"Nie udało się wysłać wiadomości po {retry_count} próbach")
        return False

    def receive(self, timeout=None):
        """
        Odbieranie wiadomości z MT4 z obsługą timeoutu.

        :param timeout: Maksymalny czas oczekiwania w sekundach (None = brak timeoutu)
        :return: Odebrana wiadomość lub None w przypadku błędu/timeoutu
        """
        if not self.is_pull_connected:
            self.logger.warning("Próba odebrania wiadomości przez niepołączone gniazdo PULL")
            return None

        start_time = time.time()
        while self.active:
            try:
                # Sprawdzenie czy mamy timeout
                if timeout is not None and (time.time() - start_time) > timeout:
                    self.logger.debug("Timeout odbierania wiadomości")
                    return None

                # Odbieranie wiadomości
                socks = dict(self.poller.poll(self.poll_timeout))
                if self.pull_sock in socks:
                    msg = self.pull_sock.recv_string(zmq.DONTWAIT)
                    self.logger.debug(f"Odebrano wiadomość: {msg}")
                    return msg

            except zmq.ZMQError as e:
                self.logger.error(f"Błąd odbierania wiadomości: {str(e)}")
                if e.errno != zmq.EAGAIN:
                    self._handle_disconnection('pull')
                    break

            time.sleep(self.sleep_delay)

        return None

    def _data_loop(self):
        """Główna pętla przetwarzania danych z rozszerzoną obsługą błędów."""
        self.logger.info("Uruchomiono główną pętlę danych")
        while self.active and not self._shutdown_initiated:
            try:
                socks = dict(self.poller.poll(self.poll_timeout))

                # Sprawdzanie gniazda PULL (odpowiedzi)
                if self.pull_sock in socks:
                    msg = self.receive()
                    if msg:
                        self._process_message(msg)

                # Sprawdzanie gniazda SUB (dane strumieniowe)
                if self.sub_sock in socks:
                    try:
                        msg = self.sub_sock.recv_string(zmq.DONTWAIT)
                        self._process_stream_message(msg)
                    except zmq.ZMQError as e:
                        if e.errno != zmq.EAGAIN:
                            self.logger.error(f"Błąd odbierania danych strumieniowych: {str(e)}")
                            self._handle_disconnection('sub')

            except Exception as e:
                self.logger.error(f"Krytyczny błąd w pętli danych: {str(e)}", exc_info=True)
                time.sleep(1)  # Zabezpieczenie przed zawieszeniem

        self.logger.info("Zakończono główną pętlę danych")

    def _process_message(self, msg):
        """Przetwarzanie wiadomości odpowiedzi (do nadpisania)."""
        raise NotImplementedError("Metoda _process_message musi być zaimplementowana w klasie pochodnej")

    def _process_stream_message(self, msg):
        """Przetwarzanie wiadomości strumieniowych (do nadpisania)."""
        raise NotImplementedError("Metoda _process_stream_message musi być zaimplementowana w klasie pochodnej")

    def shutdown(self):
        """Bezpieczne zamknięcie wszystkich zasobów."""
        if self._shutdown_initiated:
            return

        self.logger.info("Rozpoczęcie procedury zamykania...")
        self._shutdown_initiated = True
        self.active = False

        # Zamknięcie monitorów
        for monitor in self._resources.get('monitors', []):
            try:
                if not monitor.closed:
                    monitor.close()
            except Exception as e:
                self.logger.error(f"Błąd zamykania monitora: {str(e)}")

        # Zamknięcie wątków
        for thread in self._resources.get('threads', []):
            try:
                if thread.is_alive():
                    thread.join(timeout=2.0)
                    if thread.is_alive():
                        self.logger.warning(f"Wątek {thread.name} nie zakończył się w wymaganym czasie")
            except Exception as e:
                self.logger.error(f"Błąd zamykania wątku {thread.name}: {str(e)}")

        # Zamknięcie gniazd
        self._cleanup_sockets()

        # Zamknięcie kontekstu
        try:
            self.context.destroy(linger=0)
        except Exception as e:
            self.logger.error(f"Błąd niszczenia kontekstu: {str(e)}")

        self.logger.info("Procedura zamykania zakończona")

    def _cleanup_sockets(self):
        """Bezpieczne czyszczenie gniazd."""
        for sock in [self.push_sock, self.pull_sock, self.sub_sock]:
            try:
                if sock and not sock.closed:
                    sock.close()
            except Exception as e:
                self.logger.error(f"Błąd zamykania gniazda: {str(e)}")

    def _cleanup_resources(self):
        """Czyszczenie wszystkich zasobów (używane przy ponownych próbach)."""
        self._cleanup_sockets()
        if hasattr(self, 'context'):
            try:
                self.context.destroy()
            except:
                pass

    @property
    def is_push_connected(self):
        """Czy gniazdo PUSH jest połączone."""
        return self._socket_status['push']['connected']

    @property
    def is_pull_connected(self):
        """Czy gniazdo PULL jest połączone."""
        return self._socket_status['pull']['connected']

    @property
    def is_sub_connected(self):
        """Czy gniazdo SUB jest połączone."""
        return self._socket_status['sub']['connected']

    @property
    def connection_status(self):
        """Current connection state"""
        return {
            'client_id': self.client_id,
            'push': self._socket_status['push'],
            'pull': self._socket_status['pull'],
            'sub': self._socket_status['sub'],
            'config': self.config,
            'active': self.active,
            'shutdown_initiated': self._shutdown_initiated
        }