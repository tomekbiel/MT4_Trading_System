# -*- coding: utf-8 -*-
"""
MT4 Base Connector - Core connection handler with socket monitoring

This module provides a robust base class for establishing and managing connections
with MetaTrader 4 (MT4) through ZeroMQ sockets. It handles the low-level communication,
connection monitoring, and error recovery, allowing derived classes to focus on
specific trading functionality.

Key Features:
- Automatic socket monitoring and reconnection
- Thread-safe operations for concurrent access
- Configurable network parameters and timeouts
- Support for multiple message patterns (PUSH/PULL, PUB/SUB)
- Comprehensive error handling and logging

Typical usage involves creating a subclass that implements the abstract methods
for processing incoming messages and stream data.

Example:
    class MyMT4Connector(MT4BaseConnector):
        def _process_message(self, msg):
            # Handle incoming commands
            pass

        def _process_stream_message(self, msg):
            # Handle streaming data
            pass
"""

import zmq
import time
import logging
import json
import os
import datetime
from threading import Thread, Event
from zmq.utils.monitor import recv_monitor_message


class MT4BaseConnector:
    """
    Base class for MT4 connection management using ZeroMQ.
    
    This class provides the foundation for communicating with MetaTrader 4 through
    ZeroMQ sockets. It handles the connection lifecycle, message routing, and error
    recovery, while delegating message processing to derived classes.
    
    The connector uses three main socket types:
    - PUSH: For sending commands to MT4
    - PULL: For receiving responses from MT4
    - SUB: For receiving streaming data from MT4
    
    Attributes:
        config (dict): Configuration parameters for the connection
        context (zmq.Context): ZeroMQ context
        push_sock (zmq.Socket): Socket for sending commands
        pull_sock (zmq.Socket): Socket for receiving responses
        sub_sock (zmq.Socket): Socket for receiving streaming data
        symbols (list): List of available trading symbols
        timeframes (dict): Available timeframes and their properties
        
    Note:
        This is an abstract base class. Subclasses must implement:
        - _process_message()
        - _process_stream_message()
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
        """
        Initialize the MT4 connector with configuration.
        
        Args:
            client_id (str, optional): Unique identifier for this client. If not provided,
                                    a timestamp-based ID will be generated.
            config (dict, optional): Configuration overrides. Will be merged with defaults.
            **kwargs: Additional configuration parameters as keyword arguments.
            
        The configuration is loaded in this order:
        1. Default configuration from the module
        2. Values from the config dictionary (if provided)
        3. Values from keyword arguments (highest precedence)
        """
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

        # 🔄 Port mapping according to MQL4 DWX Server: MT4 PULL <-- Python PUSH, MT4 PUSH --> Python PULL
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
        """
        Establish connection with an automatic retry mechanism.
        
        This method attempts to establish all necessary socket connections and starts
        the monitoring threads. If the connection fails, it will retry according to
        the configured retry settings.
        
        Raises:
            ConnectionError: If all connection attempts fail
        """
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
        """
        Initialize and configure all ZeroMQ sockets.
        
        This method sets up three main sockets:
        - PUSH socket for sending commands to MT4
        - PULL socket for receiving responses
        - SUB socket for receiving streaming data
        
        Also initializes the poller for monitoring socket events.
        
        Raises:
            zmq.ZMQError: If socket initialization fails
        """
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
        """
        Load trading symbols and timeframes from the configuration.
        
        Args:
            config_path (str, optional): Path to the configuration file. If not provided,
                                     looks for 'config/symbols.json' relative to the module.
        
        On failure, falls back to default values for symbols and timeframes.
        """
        try:
            # 🛠️ Determine an absolute path to the project directory
            base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
            config_file = os.path.join(base_dir, 'config', 'symbols.json') if config_path is None else config_path

            with open(config_file, "r") as f:
                data = json.load(f)

            self.symbols = data.get("symbols", [])
            self.timeframes = data.get("timeframes", {})

            self.logger.info(f"Loaded symbols: {len(self.symbols)}, timeframes: {len(self.timeframes)}")

        except Exception as e:
            self.logger.warning(f"Error loading symbols/timeframes from JSON: {e}")
            self.symbols = ['US.100+', 'EURUSD+', 'GOLDs+']  # fallback
            self.timeframes = {"M15": {"max_days": 10}}  # fallback

    def get_max_history_range(self, timeframe):
        """
        Get the maximum historical data range for a given timeframe.
        
        Args:
            timeframe (str): The timeframe to check (e.g., 'M15', 'H1')
            
        Returns:
            str: A human-readable string describing the maximum historical range
                 (e.g., '10 days', '6 months', '2 years')
                 
        Note:
            The actual range depends on the configuration in symbols.json
        """
        tf_data = self.timeframes.get(timeframe)
        if not tf_data:
            return "Unknown timeframe"

        if "max_days" in tf_data:
            val = tf_data['max_days']
            return f"{val} days" if isinstance(val, int) else val
        elif "max_months" in tf_data:
            val = tf_data['max_months']
            return f"{val} months" if isinstance(val, int) else val
        elif "max_years" in tf_data:
            val = tf_data['max_years']
            return f"{val} years" if isinstance(val, int) else val
        else:
            return "Unspecified range"

    def _start_data_thread(self):
        """
        Start the background thread for processing incoming messages.
        
        This thread runs the _data_loop method which continuously polls for
        incoming messages and dispatches them to the appropriate handlers.
        """
        self.data_thread = Thread(target=self._data_loop, name="DataThread", daemon=True)
        self.data_thread.start()
        self._resources['threads'].append(self.data_thread)
        self.logger.debug("Data thread started")

    def _init_socket_monitoring(self):
        """
        Initialize socket monitoring for all active sockets.
        
        Creates monitoring threads for each socket to track connection state
        and automatically handle reconnection when needed.
        
        Note:
            Each socket gets its own monitoring thread that runs in the background.
        """
        try:
            # PUSH socket monitoring
            push_monitor = self.push_sock.get_monitor_socket()
            push_thread = Thread(target=self._monitor_loop, args=(push_monitor, 'push'),
                                 name="PushMonitorThread", daemon=True)
            push_thread.start()
            self._resources['monitors'].append(push_monitor)
            self._resources['threads'].append(push_thread)

            # PULL socket monitoring
            pull_monitor = self.pull_sock.get_monitor_socket()
            pull_thread = Thread(target=self._monitor_loop, args=(pull_monitor, 'pull'),
                                 name="PullMonitorThread", daemon=True)
            pull_thread.start()
            self._resources['monitors'].append(pull_monitor)
            self._resources['threads'].append(pull_thread)

            # SUB socket monitoring
            sub_monitor = self.sub_sock.get_monitor_socket()
            sub_thread = Thread(target=self._monitor_loop, args=(sub_monitor, 'sub'),
                                name="SubMonitorThread", daemon=True)
            sub_thread.start()
            self._resources['monitors'].append(sub_monitor)
            self._resources['threads'].append(sub_thread)

            self.logger.info("Socket monitoring started")

        except Exception as e:
            self.logger.error(f"Error initializing socket monitoring: {str(e)}")
            raise

    def _monitor_loop(self, monitor_socket, socket_type):
        """Socket event monitoring loop."""
        self.logger.debug(f"Started monitoring {socket_type} socket")
        while self.active and not self._shutdown_initiated:
            try:
                event = recv_monitor_message(monitor_socket, flags=zmq.NOBLOCK)
                if event:
                    event_name = self.SOCKET_EVENTS.get(event['event'], "UNKNOWN")
                    self.logger.debug(f"Socket {socket_type} event: {event_name} (addr: {event['endpoint']})")

                    # Update socket status
                    self._update_socket_status(socket_type, event['event'], event_name)

                    # Handle critical events
                    if event['event'] == zmq.EVENT_DISCONNECTED:
                        self.logger.warning(f"Socket {socket_type} disconnected! Attempting to reconnect...")
                        self._handle_disconnection(socket_type)

            except zmq.ZMQError as e:
                if e.errno != zmq.EAGAIN:
                    self.logger.error(f"Error monitoring {socket_type} socket: {str(e)}")
                time.sleep(0.1)
            except Exception as e:
                self.logger.error(f"Unexpected error in {socket_type} monitor loop: {str(e)}")
                time.sleep(1)

        self.logger.debug(f"Stopped monitoring {socket_type} socket")

    def _update_socket_status(self, socket_type, event_code, event_name):
        """Update socket status based on an event."""
        if socket_type in self._socket_status:
            self._socket_status[socket_type]['last_event'] = {
                'code': event_code,
                'name': event_name,
                'timestamp': time.time()
            }

            # Update connection flag
            if event_code == zmq.EVENT_CONNECTED:
                self._socket_status[socket_type]['connected'] = True
            elif event_code in [zmq.EVENT_DISCONNECTED, zmq.EVENT_CLOSED]:
                self._socket_status[socket_type]['connected'] = False

    def _handle_disconnection(self, socket_type):
        """Handle socket disconnection."""
        if socket_type == 'push':
            self._reconnect_socket('push', self.push_sock, self.push_port)
        elif socket_type == 'pull':
            self._reconnect_socket('pull', self.pull_sock, self.pull_port)
        elif socket_type == 'sub':
            self._reconnect_socket('sub', self.sub_sock, self.sub_port)

    # 🛠️ FIX: Modified _reconnect_socket method with old socket unregistered
    def _reconnect_socket(self, socket_type, socket, port):
        retry_count = 0
        while retry_count < self.max_retries and self.active and not self._shutdown_initiated:
            try:
                self.logger.info(f"🔁 Reconnect {socket_type} ({retry_count + 1}/{self.max_retries})")

                # ➕ DEBUG: poller state before
                self.logger.debug(f"📋 Socket poller BEFORE reconnect: {self.poller.sockets}")

                # 🧹 Unregister an old socket from poller - only if exists
                if socket_type in ['pull', 'sub']:
                    try:
                        if socket in dict(self.poller.sockets):
                            self.logger.debug(f"🧽 Unregistering old {socket_type} socket from poller")
                            self.poller.unregister(socket)
                        else:
                            self.logger.debug(f"ℹ️ Socket {socket_type} was not registered in poller")
                    except Exception as e:
                        self.logger.warning(f"❗ Error unregistering {socket_type}: {e}")

                if not socket.closed:
                    socket.close()
                    self.logger.debug(f"✅ Closed old {socket_type} socket")

                # 🔧 New socket
                new_socket = self.context.socket(
                    zmq.PUSH if socket_type == 'push' else
                    zmq.PULL if socket_type == 'pull' else zmq.SUB
                )
                new_socket.setsockopt(zmq.LINGER, 0)
                new_socket.connect(f"{self.protocol}://{self.host}:{port}")
                self.logger.debug(f"{socket_type.upper()} socket closed? {new_socket.closed}")

                if socket_type == 'sub':
                    new_socket.setsockopt_string(zmq.SUBSCRIBE, '')

                # 🔄 Update references
                if socket_type == 'push':
                    self.push_sock = new_socket
                elif socket_type == 'pull':
                    self.pull_sock = new_socket
                else:
                    self.sub_sock = new_socket

                # 📬 Register new socket in poller
                if socket_type in ['pull', 'sub']:
                    try:
                        self.poller.register(new_socket, zmq.POLLIN)
                        self.logger.debug(f"✅ Registered new {socket_type} socket in poller: {new_socket}")
                    except Exception as e:
                        self.logger.error(f"❌ Error registering new {socket_type} in poller: {e}")

                self.logger.debug(f"📋 Socket poller AFTER reconnect: {self.poller.sockets}")
                self.logger.info(f"✅ {socket_type} socket reconnection successful")
                return True

            except zmq.ZMQError as e:
                retry_count += 1
                self.logger.error(f"❌ {socket_type} reconnect error: {str(e)}")
                if retry_count < self.max_retries:
                    time.sleep(self.retry_delay)

        self.logger.error(f"🛑 Failed to connect {socket_type} after {retry_count} attempts")
        return False

    def send(self, message):
        """
        Send a message to MT4 with extended error handling.

        Args:
            message (str): The message to be sent to MT4. Should be a valid JSON string
                         or a dictionary that can be serialized to JSON.
            
        Returns:
            bool: True if the message was sent successfully, False otherwise.
            
        Note:
            This method is thread-safe and can be called from multiple threads.
        """
        if not self.is_push_connected:
            self.logger.warning("Attempt to send message through unconnected PUSH socket")
            return False

        retry_count = 0
        while retry_count <= self.max_retries and self.active:
            try:
                self.push_sock.send_string(message, zmq.DONTWAIT)
                self.logger.debug(f"Sent message: {message}")
                return True

            except zmq.ZMQError as e:
                retry_count += 1
                self.logger.error(f"Send error (attempt {retry_count}/{self.max_retries}): {str(e)}")

                if e.errno == zmq.EAGAIN:  # Temporary error
                    time.sleep(self.retry_delay)
                else:  # Critical error
                    self._handle_disconnection('push')
                    break

        self.logger.error(f"Failed to send message after {retry_count} attempts")
        return False

    def receive(self, timeout=None):
        """
        Receive a message from MT4 with timeout handling.

        Args:
            timeout (float, optional): Maximum time to wait for a message in seconds.
                                    If None, waits indefinitely. Defaults to None.
                                    
        Returns:
            str or None: The received message as a string, or None if no message
                       was received before the timeout or if an error occurred.
                       
        Note:
            This method is non-blocking if timeout is set to 0.
        """
        if not self.is_pull_connected:
            self.logger.warning("Attempt to receive message through unconnected PULL socket")
            return None

        start_time = time.time()
        while self.active:
            try:
                # Check for timeout
                if timeout is not None and (time.time() - start_time) > timeout:
                    self.logger.debug("Receive timeout")
                    return None

                # Receive amessage
                socks = dict(self.poller.poll(self.poll_timeout))
                if self.pull_sock in socks:
                    msg = self.pull_sock.recv_string(zmq.DONTWAIT)
                    self.logger.debug(f"Received message: {msg}")
                    return msg

            except zmq.ZMQError as e:
                self.logger.error(f"Receive error: {str(e)}")
                if e.errno != zmq.EAGAIN:
                    self._handle_disconnection('pull')
                    break

            time.sleep(self.sleep_delay)

        return None

    def _data_loop(self):
        """
        Main processing loop for handling incoming messages.
        
        This method runs in a separate thread and continuously polls the sockets
        for new messages. When a message is received, it's dispatched to the
        appropriate handler based on the socket type.
        
        The loop continues running until the connection is closed or an
        unrecoverable error occurs.
        """
        self.logger.info("Started main data loop")
        while self.active and not self._shutdown_initiated:
            try:
                socks = dict(self.poller.poll(self.poll_timeout))

                # Check PULL socket (responses)
                if self.pull_sock in socks:
                    msg = self.receive()
                    if msg:
                        self._process_message(msg)

                # Check SUB socket (stream data)
                if self.sub_sock in socks:
                    try:
                        msg = self.sub_sock.recv_string(zmq.DONTWAIT)
                        self._process_stream_message(msg)
                    except zmq.ZMQError as e:
                        if e.errno != zmq.EAGAIN:
                            self.logger.error(f"Stream data receive error: {str(e)}")
                            self._handle_disconnection('sub')

            except Exception as e:
                self.logger.error(f"Critical error in data loop: {str(e)}", exc_info=True)
                time.sleep(1)  # Protection against hanging

        self.logger.info("Stopped main data loop")

    def _process_message(self, msg):
        """
        Process an incoming message from MT4.
        
        This is an abstract method that must be implemented by subclasses to handle
        command responses received through the PULL socket.
        
        Args:
            msg (str): The received message, typically a JSON string that needs to be parsed.
            
        Raises:
            NotImplementedError: If not overridden by a subclass
        """
        raise NotImplementedError("_process_message method must be implemented in child class")

    def _process_stream_message(self, msg):
        """
        Process an incoming stream message from MT4.
        
        This is an abstract method that must be implemented by subclasses to handle
        streaming data received through the SUB socket.
        
        Args:
            msg (str): The received stream message, typically a JSON string.
            
        Raises:
            NotImplementedError: If not overridden by a subclass
        """
        raise NotImplementedError("_process_stream_message method must be implemented in child class")

    def shutdown(self):
        """
        Gracefully shut down the connector and release all resources.
        
        This method should be called when the connector is no longer needed to
        ensure proper cleanup of sockets and threads.
        """
        if self._shutdown_initiated:
            return

        self.logger.info("Starting shutdown procedure...")
        self._shutdown_initiated = True
        self.active = False

        # Close monitors
        for monitor in self._resources.get('monitors', []):
            try:
                if not monitor.closed:
                    monitor.close()
            except Exception as e:
                self.logger.error(f"Monitor close error: {str(e)}")

        # Close threads
        for thread in self._resources.get('threads', []):
            try:
                if thread.is_alive():
                    thread.join(timeout=2.0)
                    if thread.is_alive():
                        self.logger.warning(f"Thread {thread.name} did not terminate in required time")
            except Exception as e:
                self.logger.error(f"Thread {thread.name} close error: {str(e)}")

        # Close sockets
        self._cleanup_sockets()

        # Close context
        try:
            self.context.destroy(linger=0)
        except Exception as e:
            self.logger.error(f"Context destroy error: {str(e)}")

        self.logger.info("Shutdown procedure completed")

    def _cleanup_sockets(self):
        """Safely clean up sockets."""
        for sock in [self.push_sock, self.pull_sock, self.sub_sock]:
            try:
                if sock and not sock.closed:
                    sock.close()
            except Exception as e:
                self.logger.error(f"Socket close error: {str(e)}")

    def _cleanup_resources(self):
        """Clean up all resources (used during retries)."""
        self._cleanup_sockets()
        if hasattr(self, 'context'):
            try:
                self.context.destroy()
            except:
                pass

    @property
    def is_push_connected(self):
        """Is the PUSH socket connected?"""
        return self._socket_status['push']['connected']

    @property
    def is_pull_connected(self):
        """Is the PULL socket connected?"""
        return self._socket_status['pull']['connected']

    @property
    def is_sub_connected(self):
        """Is the SUB socket connected?"""
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