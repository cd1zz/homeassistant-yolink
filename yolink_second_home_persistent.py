import uuid
import json
import time
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import aiohttp
import paho.mqtt.client as mqtt
import appdaemon.plugins.hass.hassapi as hass
from collections import deque


class ConnectionState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    FAILED = "failed"


class ConnectionMethod(Enum):
    TCP = "tcp"
    WEBSOCKET = "websocket"


class ApiError(Exception):
    """Custom exception for API errors with error codes"""
    def __init__(self, message, error_code=None):
        super().__init__(message)
        self.error_code = error_code


class YoLinkDoorSensor(hass.Hass):
    def initialize(self):
        self.validate_config()
        
        # API Configuration
        self.uaid = self.args["uaid"]
        self.secret_key = self.args["secret_key"]
        self.api_url = self.args["api_url"]
        self.api_token_url = self.args["api_token_url"]
        
        # MQTT Configuration - Support both TCP and WebSocket
        self.mqtt_host = self.args["mqtt_host"]
        self.mqtt_tcp_port = self.args.get("mqtt_port", 8003)
        self.mqtt_ws_port = self.args.get("mqtt_ws_port", 8004)
        self.preferred_connection = ConnectionMethod.WEBSOCKET  # WebSocket is now primary
        self.current_connection_method = None
        
        # Token Management
        self.access_token = None
        self.refresh_token = None
        self.token_expires_at = None
        self.token_refresh_buffer = 300  # Refresh 5 minutes before expiry
        
        # Device Management
        self.home_id = None
        self.device_names = {}
        
        # MQTT State Management
        self.mqtt_client = None
        self.connection_state = ConnectionState.DISCONNECTED
        
        # YoLink MQTT Rate Limiting (per documentation)
        self.max_concurrent_connections = 5  # YoLink limit
        self.max_connections_per_window = 10  # YoLink limit: 10 connections in 5 minutes
        self.rate_limit_window = 5 * 60  # 5 minutes in seconds
        self.connection_timestamps = deque(maxlen=self.max_connections_per_window)
        self.active_connections = 0
        
        # MQTT Reconnection Strategy
        self.mqtt_reconnect_interval = 60
        self.connection_retry_handle = None
        self.connection_timeout_handle = None  # Add timeout handle
        self.mqtt_connection_attempts = 0
        self.mqtt_max_retries = 5
        self.mqtt_reconnect_backoff = 2
        self.websocket_failed = False  # Track if WebSocket connection has failed
        
        # API Rate Limiting
        self.api_semaphore = asyncio.Semaphore(5)  # Max 5 concurrent API requests
        self.api_request_history = deque(maxlen=100)  # Track for 100/5min limit
        self.device_request_history = {}  # Track per-device requests (6/min limit)
        
        self.log("Starting YoLink Door Sensor integration for second home...")
        
        # Initialize in sequence
        self.run_in(self.get_access_token, 0)
        self.run_in(self.get_home_id, 3)
        self.run_in(self.get_device_list, 6)
        self.run_in(self.setup_mqtt_client, 8)

    def validate_config(self):
        """Validate required configuration parameters"""
        required_args = ["uaid", "secret_key", "api_url", "api_token_url", "mqtt_host"]
        missing = [arg for arg in required_args if not self.args.get(arg)]
        if missing:
            raise ValueError(f"Missing required configuration: {missing}")

    def can_make_api_request(self):
        """Check if we can make an API request based on rate limits"""
        now = datetime.now()
        
        # Clean old requests (5 minute window)
        while self.api_request_history and (now - self.api_request_history[0]).total_seconds() > 300:
            self.api_request_history.popleft()
        
        # Check 100 requests per 5 minutes limit
        if len(self.api_request_history) >= 100:
            oldest = self.api_request_history[0]
            seconds_until_slot = 300 - (now - oldest).total_seconds()
            self.log(f"API rate limit reached. Need to wait {seconds_until_slot:.1f} seconds", level="WARNING")
            return False
        
        return True

    def can_make_device_request(self, device_id):
        """Check if we can make a request to specific device (6 requests per minute limit)"""
        now = datetime.now()
        
        if device_id not in self.device_request_history:
            self.device_request_history[device_id] = deque(maxlen=6)
        
        device_history = self.device_request_history[device_id]
        
        # Clean old requests (1 minute window)
        while device_history and (now - device_history[0]).total_seconds() > 60:
            device_history.popleft()
        
        # Check 6 requests per minute limit
        if len(device_history) >= 6:
            oldest = device_history[0]
            seconds_until_slot = 60 - (now - oldest).total_seconds()
            self.log(f"Device {device_id} rate limit reached. Need to wait {seconds_until_slot:.1f} seconds", level="WARNING")
            return False
        
        return True

    async def make_api_request(self, payload, endpoint=None):
        """Centralized API request handler with rate limiting and error handling"""
        if not self.can_make_api_request():
            raise ApiError("API rate limit exceeded", "010301")
        
        if not self.access_token:
            raise ApiError("No access token available")
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        url = endpoint or self.api_url
        
        async with self.api_semaphore:
            try:
                # Record the API request
                self.api_request_history.append(datetime.now())
                
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, headers=headers, json=payload) as response:
                        resp_text = await response.text()
                        
                        if response.status == 200:
                            json_response = json.loads(resp_text)
                            error_code = json_response.get("code")
                            
                            if error_code and error_code != "000000":
                                self.handle_api_error_code(error_code)
                                raise ApiError(f"API error: {error_code}", error_code)
                            
                            return json_response
                        else:
                            raise ApiError(f"HTTP {response.status}: {resp_text}")
                            
            except aiohttp.ClientError as e:
                raise ApiError(f"Network error: {e}")
            except json.JSONDecodeError as e:
                raise ApiError(f"Invalid JSON response: {e}")

    def handle_api_error_code(self, error_code):
        """Handle specific YoLink API error codes"""
        if error_code == "020104":
            self.log("Rate limited: >6 requests to device in 1 minute", level="WARNING")
        elif error_code == "010301":
            self.log("Rate limited: >100 requests in 5 minutes", level="WARNING")
        else:
            self.log(f"API returned error code: {error_code}", level="ERROR")

    def should_refresh_token(self):
        """Check if token needs refreshing"""
        if not self.token_expires_at:
            return True  # No expiry info, refresh to be safe
        
        return datetime.now() >= (self.token_expires_at - timedelta(seconds=self.token_refresh_buffer))

    async def get_access_token(self, kwargs=None, use_refresh=True):
        """Get access token using refresh token when available"""
        if use_refresh and hasattr(self, 'refresh_token') and self.refresh_token:
            data = {
                "grant_type": "refresh_token",
                "client_id": self.uaid,
                "refresh_token": self.refresh_token
            }
            self.log("Refreshing access token using refresh_token")
        else:
            data = {
                "grant_type": "client_credentials",
                "client_id": self.uaid,
                "client_secret": self.secret_key
            }
            self.log("Getting new access token using client_credentials")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.api_token_url, json=data) as response:
                    resp_text = await response.text()
                    
                    if response.status == 200:
                        json_response = json.loads(resp_text)
                        self.access_token = json_response.get("access_token")
                        self.refresh_token = json_response.get("refresh_token")
                        
                        # Calculate token expiration
                        expires_in = json_response.get("expires_in", 3600)  # Default 1 hour
                        self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)
                        
                        self.log(f"Access token obtained, expires at {self.token_expires_at}")
                        
                        # Schedule refresh before expiration
                        if self.token_refresh_buffer < expires_in:
                            refresh_delay = expires_in - self.token_refresh_buffer
                            self.run_in(self.refresh_access_token, refresh_delay)
                    else:
                        # If refresh token failed, try with client_credentials
                        if use_refresh and self.refresh_token:
                            self.log("Refresh token failed, trying client_credentials", level="WARNING")
                            self.refresh_token = None
                            await self.get_access_token(use_refresh=False)
                        else:
                            raise Exception(f"Failed to obtain access token: HTTP {response.status}")
                            
        except Exception as e:
            self.log(f"Access token error: {e}", level="ERROR")
            if not use_refresh:  # Don't retry infinitely
                # Schedule retry in 60 seconds
                self.run_in(self.get_access_token, 60)

    async def refresh_access_token(self, kwargs=None):
        """Refresh access token using refresh_token when possible"""
        if self.should_refresh_token():
            await self.get_access_token(use_refresh=True)
            
            # Update MQTT client credentials if connected
            if self.mqtt_client and self.connection_state == ConnectionState.CONNECTED:
                self.log("Updating MQTT client credentials with new token")
                self.mqtt_client.username_pw_set(self.access_token)

    async def get_home_id(self, kwargs=None):
        """Get home ID for MQTT topic subscription"""
        if not self.access_token:
            self.log("No access token available, scheduling retry for home ID", level="WARNING")
            self.run_in(self.get_home_id, 10)
            return
        
        payload = {
            "method": "Home.getGeneralInfo",
            "time": int(time.time() * 1000)
        }
        
        try:
            response = await self.make_api_request(payload)
            self.home_id = response.get("data", {}).get("id")
            self.log(f"Home ID obtained: {self.home_id}")
        except ApiError as e:
            self.log(f"Home ID error: {e}", level="ERROR")
            if e.error_code in ["010301", "020104"]:  # Rate limited
                self.run_in(self.get_home_id, 60)  # Retry in 1 minute
            else:
                self.run_in(self.get_home_id, 10)  # Retry in 10 seconds

    async def get_device_list(self, kwargs=None):
        """Get device list for friendly names"""
        if not self.access_token:
            self.log("No access token available, scheduling retry for device list", level="WARNING")
            self.run_in(self.get_device_list, 10)
            return
        
        payload = {
            "method": "Home.getDeviceList",
            "time": int(time.time() * 1000)
        }
        
        try:
            response = await self.make_api_request(payload)
            devices = response.get("data", {}).get("devices", [])
            
            for device in devices:
                device_id = device["deviceId"]
                device_name = device["name"].replace(" ", "_").lower()
                self.device_names[device_id] = device_name
            
            self.log(f"Device list fetched, found {len(devices)} devices")
        except ApiError as e:
            self.log(f"Device list error: {e}", level="ERROR")
            if e.error_code in ["010301", "020104"]:  # Rate limited
                self.run_in(self.get_device_list, 60)
            else:
                self.run_in(self.get_device_list, 10)

    def can_connect_mqtt(self):
        """Check if we can establish a new MQTT connection based on YoLink's rate limits"""
        now = datetime.now()
        
        # Remove expired timestamps outside the 5-minute window
        while self.connection_timestamps and (now - self.connection_timestamps[0]).total_seconds() > self.rate_limit_window:
            self.connection_timestamps.popleft()
        
        # Check 10 connections per 5 minutes limit
        if len(self.connection_timestamps) >= self.max_connections_per_window:
            oldest = self.connection_timestamps[0]
            seconds_until_slot = self.rate_limit_window - (now - oldest).total_seconds()
            self.log(f"MQTT rate limit reached (10/5min). Need to wait {seconds_until_slot:.1f} seconds", level="WARNING")
            return False
        
        # Check 5 concurrent connections limit
        if self.active_connections >= self.max_concurrent_connections:
            self.log(f"Maximum concurrent MQTT connections reached ({self.max_concurrent_connections})", level="WARNING")
            return False
        
        return True

    def calculate_backoff_time(self):
        """Calculate progressive backoff time respecting YoLink limits"""
        if not self.can_connect_mqtt():
            # If we can't connect due to rate limits, wait for the rate limit window plus buffer
            return self.rate_limit_window + 30
        
        # Progressive backoff: 60s, 120s, 240s, 300s (max)
        backoff = min(300, 60 * (2 ** (self.mqtt_connection_attempts - 1)))
        return backoff

    def determine_connection_method(self):
        """Determine which connection method to use - WebSocket first, TCP fallback"""
        # If WebSocket hasn't failed yet, or we haven't exceeded retry limit, try WebSocket first
        if not self.websocket_failed or (self.mqtt_connection_attempts <= 2):
            return ConnectionMethod.WEBSOCKET
        else:
            # After WebSocket has failed or after 2 attempts, try TCP
            return ConnectionMethod.TCP

    def setup_mqtt_client(self, kwargs=None):
        """Initialize MQTT client with TCP/WebSocket failover"""
        if not self.access_token:
            self.log("Cannot start MQTT client without access token", level="WARNING")
            self.run_in(self.setup_mqtt_client, 10)
            return
        
        if not self.can_connect_mqtt():
            backoff_time = self.calculate_backoff_time()
            self.log(f"Rate limited, retry MQTT setup in {backoff_time} seconds")
            self.run_in(self.setup_mqtt_client, backoff_time)
            return
        
        try:
            # Clean up existing client
            if self.mqtt_client:
                try:
                    self.mqtt_client.loop_stop()
                    self.mqtt_client.disconnect()
                    self.active_connections = max(0, self.active_connections - 1)
                except Exception:
                    pass
            
            # Determine connection method
            connection_method = self.determine_connection_method()
            self.current_connection_method = connection_method
            
            self.connection_state = ConnectionState.CONNECTING
            
            # Create client with appropriate transport
            if connection_method == ConnectionMethod.WEBSOCKET:
                self.mqtt_client = mqtt.Client(client_id=str(uuid.uuid4()), transport="websockets")
            else:
                self.mqtt_client = mqtt.Client(client_id=str(uuid.uuid4()))
                
            self.mqtt_client.username_pw_set(self.access_token)
            self.mqtt_client.on_connect = self.on_connect
            self.mqtt_client.on_message = self.on_message
            self.mqtt_client.on_disconnect = self.on_disconnect
            
            # Record connection attempt
            self.connection_timestamps.append(datetime.now())
            self.active_connections += 1
            
            if connection_method == ConnectionMethod.WEBSOCKET:
                self.log(f"Connecting to MQTT broker via WebSocket: {self.mqtt_host}:{self.mqtt_ws_port}")
                # Try different WebSocket paths that YoLink might use
                try:
                    # Try default /mqtt path first
                    self.mqtt_client.ws_set_options(path="/mqtt")
                    self.mqtt_client.connect_async(self.mqtt_host, self.mqtt_ws_port, 60)
                except Exception as e:
                    self.log(f"WebSocket connection failed with /mqtt path: {e}", level="WARNING")
                    try:
                        # Try root path
                        self.mqtt_client.ws_set_options(path="/")
                        self.mqtt_client.connect_async(self.mqtt_host, self.mqtt_ws_port, 60)
                    except Exception as e2:
                        self.log(f"WebSocket connection failed with / path: {e2}", level="ERROR")
                        raise e2
            else:  # TCP
                self.log(f"Connecting to MQTT broker via TCP: {self.mqtt_host}:{self.mqtt_tcp_port}")
                self.mqtt_client.connect_async(self.mqtt_host, self.mqtt_tcp_port, 60)
            
            self.mqtt_client.loop_start()
            
            # Set up connection timeout - if we don't get on_connect callback within 30 seconds, force failure
            self.connection_timeout_handle = self.run_in(self.handle_connection_timeout, 30)
            
        except Exception as e:
            self.log(f"MQTT connection error: {e}", level="ERROR")
            self.active_connections = max(0, self.active_connections - 1)
            self.connection_state = ConnectionState.FAILED
            
            # Mark WebSocket as failed if this was a WebSocket attempt
            if self.current_connection_method == ConnectionMethod.WEBSOCKET:
                self.websocket_failed = True
                self.log("WebSocket connection failed, will try TCP next", level="WARNING")
            
            self.schedule_reconnect()

    def handle_connection_timeout(self, kwargs=None):
        """Handle connection timeout - force failover if WebSocket is hanging"""
        # Clear the timeout handle since this callback is executing
        self.connection_timeout_handle = None
        
        if self.connection_state == ConnectionState.CONNECTING:
            self.log(f"Connection timeout after 30 seconds via {self.current_connection_method.value}", level="WARNING")
            
            # Clean up the hanging connection
            if self.mqtt_client:
                try:
                    self.mqtt_client.loop_stop()
                    self.mqtt_client.disconnect()
                except Exception:
                    pass
            
            self.active_connections = max(0, self.active_connections - 1)
            self.connection_state = ConnectionState.FAILED
            
            # Mark current method as failed
            if self.current_connection_method == ConnectionMethod.WEBSOCKET:
                self.websocket_failed = True
                self.log("WebSocket connection timeout, marking as failed and will try TCP", level="WARNING")
            
            # Force immediate reconnection with other method
            self.schedule_reconnect()

    def on_connect(self, client, userdata, flags, rc):
        """Handle MQTT connection result"""
        # Cancel the timeout timer since we got a connection response
        if self.connection_timeout_handle:
            try:
                self.cancel_timer(self.connection_timeout_handle)
            except Exception:
                pass
            self.connection_timeout_handle = None
        
        self.log(f"MQTT on_connect via {self.current_connection_method.value}: rc={rc}")
        
        if rc == 0:
            self.log(f"Connected to MQTT Broker via {self.current_connection_method.value}")
            self.connection_state = ConnectionState.CONNECTED
            self.mqtt_connection_attempts = 0
            
            # Reset WebSocket failed flag on successful connection
            if self.current_connection_method == ConnectionMethod.WEBSOCKET:
                self.websocket_failed = False
            
            if self.home_id:
                topic = f'yl-home/{self.home_id}/+/report'
                self.log(f"Subscribing to topic: {topic}")
                client.subscribe(topic)
            else:
                self.log("Home ID not available, retrying home ID fetch", level="WARNING")
                self.run_in(self.get_home_id, 5)
        else:
            self.log(f"Failed to connect to MQTT Broker via {self.current_connection_method.value}, return code {rc}", level="ERROR")
            self.connection_state = ConnectionState.FAILED
            self.active_connections = max(0, self.active_connections - 1)
            
            # Mark current method as failed
            if self.current_connection_method == ConnectionMethod.WEBSOCKET:
                self.websocket_failed = True
                self.log("WebSocket connection failed, will try TCP next", level="WARNING")
            
            self.schedule_reconnect()

    def on_disconnect(self, client, userdata, rc):
        """Handle MQTT disconnection"""
        self.log(f"MQTT disconnected from {self.current_connection_method.value if self.current_connection_method else 'unknown'} with result code: {rc}")
        self.connection_state = ConnectionState.DISCONNECTED
        self.active_connections = max(0, self.active_connections - 1)
        
        # Only auto-reconnect for unexpected disconnects
        if rc != 0:
            # If this was a WebSocket connection that disconnected unexpectedly, mark it as potentially unstable
            if self.current_connection_method == ConnectionMethod.WEBSOCKET and rc in [7, 104]:  # Connection lost/reset
                self.websocket_failed = True
                self.log("WebSocket connection unstable, will prefer TCP for next connection", level="WARNING")
            
            self.schedule_reconnect()

    def schedule_reconnect(self):
        """Schedule MQTT reconnection with proper backoff"""
        # Cancel any existing retry timer (but not timeout timer - that may be executing)
        if self.connection_retry_handle:
            try:
                self.cancel_timer(self.connection_retry_handle)
            except Exception:
                pass
            self.connection_retry_handle = None
        
        # Only cancel timeout timer if it hasn't executed yet
        if self.connection_timeout_handle:
            try:
                self.cancel_timer(self.connection_timeout_handle)
            except Exception:
                pass
            self.connection_timeout_handle = None
        
        self.mqtt_connection_attempts += 1
        backoff_time = self.calculate_backoff_time()
        
        if self.mqtt_connection_attempts > self.mqtt_max_retries:
            # Reset attempt counter and WebSocket failed flag after max retries
            self.mqtt_connection_attempts = 0
            self.websocket_failed = False
            self.log(f"Maximum reconnection attempts reached. Resetting and will try again in {backoff_time} seconds", level="WARNING")
        else:
            self.log(f"Scheduling MQTT reconnection in {backoff_time:.1f} seconds (attempt {self.mqtt_connection_attempts})")
        
        self.connection_retry_handle = self.run_in(self.setup_mqtt_client, backoff_time)

    def on_message(self, client, userdata, msg):
        """Process incoming MQTT messages"""
        self.log(f"MQTT message received on topic: {msg.topic}")
        
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            event = payload.get("event")
            device_id = payload.get("deviceId")
            state = payload.get("data", {}).get("state")
            
            if event == "DoorSensor.Alert":
                self.log(f"DoorSensor event: device_id={device_id}, state={state}")
                self.publish_discovery_and_state(device_id, state)
                self.set_door_sensor_state(device_id, state)
            else:
                self.log(f"Ignoring non-door event: {event}")
                
        except json.JSONDecodeError as e:
            self.log(f"Invalid JSON in MQTT message: {e}", level="ERROR")
        except Exception as e:
            self.log(f"Error processing MQTT message: {e}", level="ERROR")

    def publish_discovery_and_state(self, device_id, state):
        """Publish Home Assistant MQTT discovery and state"""
        device_name = self.device_names.get(device_id, "unknown").replace(" ", "_").lower()
        topic_suffix = f"yolink_second_home_{device_id}"  # Unique prefix for second home
        discovery_topic = f"homeassistant/binary_sensor/{topic_suffix}/config"
        state_topic = f"homeassistant/binary_sensor/{topic_suffix}/state"
        
        discovery_payload = {
            "device_class": "door",
            "name": f"YoLink Second Home {device_name} Door Sensor",
            "state_topic": state_topic,
            "payload_on": "open", 
            "payload_off": "closed",
            "value_template": "{{ value_json.state }}",
            "json_attributes_topic": state_topic,
            "unique_id": topic_suffix,
            "device": {
                "identifiers": [f"yolink_second_home_{device_id}"],
                "name": f"YoLink Second Home Door Sensor {device_name}",
                "model": "Door Sensor v1",
                "manufacturer": "YoLink"
            }
        }
        
        state_payload = {"state": state}
        
        try:
            self.log(f"Publishing discovery to: {discovery_topic}")
            self.call_service("mqtt/publish", 
                            topic=discovery_topic, 
                            payload=json.dumps(discovery_payload), 
                            qos=0, retain=True)
            
            self.log(f"Publishing state to: {state_topic}")
            self.call_service("mqtt/publish", 
                            topic=state_topic, 
                            payload=json.dumps(state_payload), 
                            qos=0, retain=True)
        except Exception as e:
            self.log(f"Error publishing to Home Assistant MQTT: {e}", level="ERROR")

    def set_door_sensor_state(self, device_id, state):
        """Set Home Assistant entity state"""
        device_name = self.device_names.get(device_id, "unknown").replace(" ", "_").lower()
        entity_id = f"binary_sensor.yolink_second_home_{device_id}_{device_name}"
        
        attributes = {
            "friendly_name": f"YoLink Second Home {device_name} Door Sensor",
            "device_class": "door",
            "home_location": "second_home",
            "connection_method": self.current_connection_method.value if self.current_connection_method else "unknown"
        }
        
        try:
            self.log(f"Setting state: {entity_id} = {state}")
            self.set_state(entity_id, state=state, attributes=attributes)
        except Exception as e:
            self.log(f"Error setting entity state: {e}", level="ERROR")

    def terminate(self):
        """Clean up resources when AppDaemon shuts down"""
        self.log("Terminating YoLink integration...")
        
        # Cancel any pending timers
        if self.connection_retry_handle:
            try:
                self.cancel_timer(self.connection_retry_handle)
            except Exception:
                pass
            self.connection_retry_handle = None
        
        if self.connection_timeout_handle:
            try:
                self.cancel_timer(self.connection_timeout_handle)
            except Exception:
                pass
            self.connection_timeout_handle = None
        
        if self.mqtt_client:
            try:
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()
                self.active_connections = max(0, self.active_connections - 1)
            except Exception as e:
                self.log(f"Error during MQTT cleanup: {e}", level="ERROR")
