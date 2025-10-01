import uuid
import json
import time
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import aiohttp
import paho.mqtt.client as mqtt
import appdaemon.plugins.hass.hassapi as hass
from collections import deque, defaultdict


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


class YolinkSensor(hass.Hass):
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
        self.preferred_connection = ConnectionMethod.WEBSOCKET
        self.current_connection_method = None

        # Token Management
        self.access_token = None
        self.refresh_token = None
        self.token_expires_at = None
        self.token_refresh_buffer = 300

        # Device Management
        self.home_id = None
        self.device_names = {}
        self.device_types = {}
        self.device_tokens = {}

        # Device status tracking
        self.device_status = {}
        self.device_last_seen = {}
        self.device_state_changed_at = {}  # Track stateChangedAt timestamps
        self.device_last_delivery = {}  # Track last delivery time for mailbox

        # Notification settings
        self.offline_notification_interval = self.args.get("offline_notification_interval", 86400)
        self.device_last_notification = {}
        self.offline_notifications_enabled = self.args.get("offline_notifications_enabled", True)

        # MQTT State Management
        self.mqtt_client = None
        self.connection_state = ConnectionState.DISCONNECTED

        # YoLink MQTT Rate Limiting
        self.max_concurrent_connections = 5
        self.max_connections_per_window = 10
        self.rate_limit_window = 5 * 60
        self.connection_timestamps = deque(maxlen=self.max_connections_per_window)
        self.active_connections = 0

        # MQTT Reconnection Strategy
        self.mqtt_reconnect_interval = 60
        self.connection_retry_handle = None
        self.connection_timeout_handle = None
        self.mqtt_connection_attempts = 0
        self.mqtt_max_retries = 5
        self.mqtt_reconnect_backoff = 2
        self.websocket_failed = False

        # API Rate Limiting
        self.api_semaphore = asyncio.Semaphore(5)
        self.api_request_history = deque(maxlen=100)
        self.device_request_history = {}

        self.log("Starting YoLink Door Sensor integration for second home...")

        # Initialize in sequence with sync wrappers
        self.log("Scheduling initial token request...")
        self.run_in(self.get_access_token_wrapper, 1)
        self.run_in(self.get_home_id_wrapper, 4)
        self.run_in(self.get_device_list_wrapper, 7)
        self.run_in(self.setup_mqtt_client, 10)
        self.run_in(self.fetch_initial_states, 13)

        # Schedule periodic status checks every 5 minutes
        self.run_every(self.check_device_status, datetime.now() + timedelta(seconds=60), 300)

        # Schedule mailbox-specific polling for delivery detection (every 2 hours during mail delivery window)
        self.run_every(self.check_mailbox_delivery, datetime.now() + timedelta(seconds=120), 7200)

    def validate_config(self):
        """Validate required configuration parameters"""
        required_args = ["uaid", "secret_key", "api_url", "api_token_url", "mqtt_host"]
        missing = [arg for arg in required_args if not self.args.get(arg)]
        if missing:
            raise ValueError(f"Missing required configuration: {missing}")

        self.log("Configuration validated:")
        for arg in required_args:
            if arg == "secret_key":
                self.log(f"  {arg}: {'***' if self.args.get(arg) else 'MISSING'}")
            else:
                self.log(f"  {arg}: {self.args.get(arg)}")

    def can_make_api_request(self):
        """Check if we can make an API request based on rate limits"""
        now = datetime.now()

        while self.api_request_history and (now - self.api_request_history[0]).total_seconds() > 300:
            self.api_request_history.popleft()

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

        while device_history and (now - device_history[0]).total_seconds() > 60:
            device_history.popleft()

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
                self.api_request_history.append(datetime.now())

                async with aiohttp.ClientSession() as session:
                    async with session.post(url, headers=headers, json=payload) as response:
                        resp_text = await response.text()

                        if response.status == 200:
                            json_response = json.loads(resp_text)
                            error_code = json_response.get("code")

                            if error_code and error_code != "000000":
                                self.log(f"API Error Response: {json.dumps(json_response, indent=2)}", level="ERROR")
                                self.log(f"Request payload was: {json.dumps(payload, indent=2)}", level="ERROR")
                                self.handle_api_error_code(error_code)
                                raise ApiError(f"API error: {error_code}", error_code)

                            return json_response
                        else:
                            self.log(f"HTTP {response.status} Error Response: {resp_text}", level="ERROR")
                            self.log(f"Request payload was: {json.dumps(payload, indent=2)}", level="ERROR")
                            raise ApiError(f"HTTP {response.status}: {resp_text}")

            except aiohttp.ClientError as e:
                self.log(f"Network error with payload: {json.dumps(payload, indent=2)}", level="ERROR")
                raise ApiError(f"Network error: {e}")
            except json.JSONDecodeError as e:
                self.log(f"Invalid JSON response: {resp_text[:1000]}", level="ERROR")
                self.log(f"Request payload was: {json.dumps(payload, indent=2)}", level="ERROR")
                raise ApiError(f"Invalid JSON response: {e}")

    def handle_api_error_code(self, error_code):
        """Handle specific YoLink API error codes"""
        error_descriptions = {
            "020104": "Rate limited: >6 requests to device in 1 minute",
            "010301": "Rate limited: >100 requests in 5 minutes",
            "010203": "Invalid parameter in API request",
            "000103": "Method not supported for this device",
            "000201": "Device offline",
            "000202": "Device not found",
            "000203": "Invalid device ID",
            "010101": "Invalid token",
            "010102": "Token expired",
            "010103": "Insufficient permissions"
        }

        description = error_descriptions.get(error_code, f"Unknown error code: {error_code}")

        if error_code in ["020104", "010301"]:
            self.log(description, level="WARNING")
        else:
            self.log(f"API Error {error_code}: {description}", level="ERROR")

    def should_refresh_token(self):
        """Check if token needs refreshing"""
        if not self.token_expires_at:
            return True

        return datetime.now() >= (self.token_expires_at - timedelta(seconds=self.token_refresh_buffer))

    # Sync wrapper functions for run_in calls
    def get_access_token_wrapper(self, kwargs=None):
        """Sync wrapper for get_access_token"""
        self.log("get_access_token_wrapper called")
        try:
            self.create_task(self.get_access_token(kwargs))
            self.log("get_access_token task created successfully")
        except Exception as e:
            self.log(f"Error creating get_access_token task: {e}", level="ERROR")

    def get_home_id_wrapper(self, kwargs=None):
        """Sync wrapper for get_home_id"""
        self.log("get_home_id_wrapper called")
        try:
            self.create_task(self.get_home_id(kwargs))
        except Exception as e:
            self.log(f"Error creating get_home_id task: {e}", level="ERROR")

    def get_device_list_wrapper(self, kwargs=None):
        """Sync wrapper for get_device_list"""
        self.log("get_device_list_wrapper called")
        try:
            self.create_task(self.get_device_list(kwargs))
        except Exception as e:
            self.log(f"Error creating get_device_list task: {e}", level="ERROR")

    def refresh_access_token_wrapper(self, kwargs=None):
        """Sync wrapper for refresh_access_token"""
        self.log("refresh_access_token_wrapper called")
        try:
            self.create_task(self.refresh_access_token(kwargs))
        except Exception as e:
            self.log(f"Error creating refresh_access_token task: {e}", level="ERROR")

    async def get_access_token(self, kwargs=None, use_refresh=True):
        """Get access token using refresh token when available"""
        self.log(f"get_access_token called with use_refresh={use_refresh}")

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

        self.log(f"Making token request to: {self.api_token_url}")
        self.log(f"Request data: {json.dumps({k: v if k != 'client_secret' else '***' for k, v in data.items()})}")

        try:
            async with aiohttp.ClientSession() as session:
                self.log("Created aiohttp session")
                async with session.post(self.api_token_url, json=data) as response:
                    self.log(f"Token request response status: {response.status}")
                    resp_text = await response.text()
                    self.log(f"Token response text: {resp_text[:15]}...")

                    if response.status == 200:
                        json_response = json.loads(resp_text)
                        self.log(f"Parsed JSON response keys: {list(json_response.keys())}")

                        self.access_token = json_response.get("access_token")
                        self.refresh_token = json_response.get("refresh_token")

                        if self.access_token:
                            self.log(f"Access token obtained: {self.access_token[:20]}...")
                        else:
                            self.log("No access_token in response!", level="ERROR")

                        expires_in = json_response.get("expires_in", 3600)
                        self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)

                        self.log(f"Access token obtained, expires at {self.token_expires_at}")

                        if self.token_refresh_buffer < expires_in:
                            refresh_delay = expires_in - self.token_refresh_buffer
                            self.run_in(self.refresh_access_token_wrapper, refresh_delay)
                    else:
                        self.log(f"HTTP error {response.status}: {resp_text}", level="ERROR")
                        if use_refresh and self.refresh_token:
                            self.log("Refresh token failed, trying client_credentials", level="WARNING")
                            self.refresh_token = None
                            await self.get_access_token(use_refresh=False)
                        else:
                            raise Exception(f"Failed to obtain access token: HTTP {response.status}")

        except Exception as e:
            self.log(f"Access token error: {e}", level="ERROR")
            import traceback
            self.log(f"Traceback: {traceback.format_exc()}", level="ERROR")
            if not use_refresh:
                self.log("Scheduling token retry in 60 seconds")
                self.run_in(self.get_access_token_wrapper, 60)

    async def refresh_access_token(self, kwargs=None):
        """Refresh access token using refresh_token when possible"""
        if self.should_refresh_token():
            await self.get_access_token(use_refresh=True)

            if self.mqtt_client and self.connection_state == ConnectionState.CONNECTED:
                self.log("Updating MQTT client credentials with new token")
                self.mqtt_client.username_pw_set(self.access_token)

    async def get_home_id(self, kwargs=None):
        """Get home ID for MQTT topic subscription"""
        if not self.access_token:
            self.log("No access token available, scheduling retry for home ID", level="WARNING")
            self.run_in(self.get_home_id_wrapper, 10)
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
            if e.error_code in ["010301", "020104"]:
                self.run_in(self.get_home_id_wrapper, 60)
            else:
                self.run_in(self.get_home_id_wrapper, 10)

    async def get_device_list(self, kwargs=None):
        """Get device list for friendly names, device types, and tokens"""
        if not self.access_token:
            self.log("No access token available, scheduling retry for device list", level="WARNING")
            self.run_in(self.get_device_list_wrapper, 10)
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
                device_type = device.get("type", "Unknown")
                device_token = device.get("token")

                self.device_names[device_id] = device_name
                self.device_types[device_id] = device_type
                self.device_tokens[device_id] = device_token

                self.log(f"Found device: {device_name} (ID: {device_id}, Type: {device_type})")

                # Initialize online status entity for non-Hub devices
                if device_type != "Hub":
                    self.update_device_online_status(device_id, True, mark_sensor_unavailable=False)

            self.log(f"Device list fetched, found {len(devices)} devices")
        except ApiError as e:
            self.log(f"Device list error: {e}", level="ERROR")
            if e.error_code in ["010301", "020104"]:
                self.run_in(self.get_device_list_wrapper, 60)
            else:
                self.run_in(self.get_device_list_wrapper, 10)

    async def get_device_status(self, device_id):
        """Poll device status via API"""
        device_name = self.device_names.get(device_id, "unknown")
        device_type = getattr(self, 'device_types', {}).get(device_id, "Unknown")
        device_token = getattr(self, 'device_tokens', {}).get(device_id)

        if device_type == "Hub":
            self.log(f"[API STATUS] Skipping {device_name} - Hub devices don't support getState API")
            return {"online": True, "raw_response": None, "last_report": None}

        if not device_token:
            self.log(f"[API STATUS] No device token available for {device_name}", level="ERROR")
            return None

        if not self.can_make_device_request(device_id):
            self.log(f"[API STATUS] Rate limited for {device_name}, skipping status check", level="WARNING")
            return None

        type_to_method = {
            "DoorSensor": "DoorSensor.getState",
            "THSensor": "THSensor.getState",
            "LeakSensor": "LeakSensor.getState",
            "MotionSensor": "MotionSensor.getState",
            "Switch": "Switch.getState",
            "Outlet": "Outlet.getState",
            "Manipulator": "Manipulator.getState",
            "SirenAlarm": "SirenAlarm.getState",
            "VibrationSensor": "VibrationSensor.getState"
        }

        method = type_to_method.get(device_type)

        if not method:
            self.log(f"[API STATUS] Unknown device type for {device_id} (type: {device_type}), skipping status check")
            return None

        payload = {
            "method": method,
            "targetDevice": device_id,
            "token": device_token,
            "time": int(time.time() * 1000)
        }

        try:
            if device_id in self.device_request_history:
                self.device_request_history[device_id].append(datetime.now())

            response = await self.make_api_request(payload)
            data = response.get("data", {})

            is_online = data.get("online", False)
            report_at = data.get("reportAt")
            state_data = data.get("state", {})

            self.log(f"[API STATUS] Device {device_name} status: online={is_online}, reportAt={report_at}")

            last_report_ms = None
            if report_at:
                try:
                    report_dt = datetime.fromisoformat(report_at.replace('Z', '+00:00'))
                    last_report_ms = int(report_dt.timestamp() * 1000)
                except Exception as e:
                    self.log(f"[API STATUS] Could not parse reportAt timestamp: {e}", level="WARNING")

            return {
                "online": is_online,
                "raw_response": data,
                "last_report": last_report_ms,
                "state": state_data
            }

        except ApiError as e:
            self.log(f"[API STATUS] Error getting status for {device_name}: {e}", level="ERROR")
            return None

    async def poll_device_status(self, device_id):
        """Poll a specific device's status via API"""
        status = await self.get_device_status(device_id)

        if status is not None:
            is_online = status.get("online", False)

            # Update online status - for LoRa devices, don't mark sensor entities unavailable
            # These devices only transmit on events, so lack of recent MQTT messages is normal
            self.update_device_online_status(device_id, is_online, mark_sensor_unavailable=False)

            if status.get("last_report"):
                report_time = datetime.fromtimestamp(status["last_report"] / 1000)
                self.device_last_seen[device_id] = report_time

            # Track stateChangedAt for DoorSensors to detect delivery events
            device_type = self.device_types.get(device_id)
            if device_type == "DoorSensor":
                state_data = status.get("state", {})
                state_changed_at = state_data.get("stateChangedAt")

                if state_changed_at:
                    self.check_delivery_event(device_id, state_changed_at)

    def check_delivery_event(self, device_id, state_changed_at_ms):
        """Check if stateChangedAt indicates a new delivery event"""
        device_name = self.device_names.get(device_id, "unknown")
        previous_state_changed_at = self.device_state_changed_at.get(device_id)

        # First time seeing this device or state changed
        if previous_state_changed_at is None or state_changed_at_ms > previous_state_changed_at:
            self.device_state_changed_at[device_id] = state_changed_at_ms

            # Convert to datetime
            state_changed_dt = datetime.fromtimestamp(state_changed_at_ms / 1000)

            # If this is a new state change (not first run), update last delivery
            if previous_state_changed_at is not None:
                self.device_last_delivery[device_id] = state_changed_dt
                self.log(f"[DELIVERY DETECTED] {device_name} opened at {state_changed_dt.strftime('%Y-%m-%d %H:%M:%S')}")

                # Update the last delivery entity
                self.update_last_delivery_entity(device_id, state_changed_dt)
            else:
                # First run - initialize with existing timestamp
                self.log(f"[DELIVERY INIT] {device_name} last opened at {state_changed_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                self.device_last_delivery[device_id] = state_changed_dt
                self.update_last_delivery_entity(device_id, state_changed_dt)

    def update_last_delivery_entity(self, device_id, delivery_time):
        """Update Home Assistant entity with last delivery timestamp"""
        device_name = self.device_names.get(device_id, "unknown").replace(" ", "_").lower()
        entity_id = f"sensor.yolink_second_home_{device_id}_last_delivery"

        attributes = {
            "friendly_name": f"YoLink Second Home {device_name} Last Delivery",
            "device_class": "timestamp",
            "device_id": device_id,
            "home_location": "second_home",
            "icon": "mdi:mailbox"
        }

        try:
            # Set state as ISO 8601 timestamp
            self.set_state(entity_id, state=delivery_time.isoformat(), attributes=attributes)
            self.log(f"Updated {entity_id} to {delivery_time.isoformat()}")
        except Exception as e:
            self.log(f"Error updating last delivery entity: {e}", level="ERROR")

    def check_mailbox_delivery(self, kwargs=None):
        """Poll mailbox sensors for delivery detection - runs every 2 hours"""
        now = datetime.now()
        current_hour = now.hour

        # Only poll during typical mail delivery hours (8 AM - 8 PM)
        if current_hour < 8 or current_hour > 20:
            self.log(f"[MAILBOX POLL] Skipping - outside delivery hours (current: {current_hour}:00)")
            return

        self.log(f"[MAILBOX POLL] Checking mailbox sensors for delivery at {now.strftime('%H:%M:%S')}")

        for device_id, device_type in self.device_types.items():
            device_name = self.device_names.get(device_id, "unknown")

            # Only check DoorSensors with "mailbox" in the name
            if device_type == "DoorSensor" and "mailbox" in device_name.lower():
                self.log(f"[MAILBOX POLL] Checking {device_name} for delivery")
                self.create_task(self.poll_device_status(device_id))

    def fetch_initial_states(self, kwargs=None):
        """Fetch initial states for all devices on startup"""
        self.log("Fetching initial device states...")

        if not hasattr(self, 'device_types'):
            self.log("Device types not available yet, retrying in 5 seconds")
            self.run_in(self.fetch_initial_states, 5)
            return

        for device_id, device_type in self.device_types.items():
            if device_type != "Hub":
                self.log(f"Fetching initial state for {device_id} ({device_type})")
                self.create_task(self.poll_device_status(device_id))

    def check_device_status(self, kwargs=None):
        """Periodically check device status via API - for LoRa devices, use API not time-based detection"""
        now = datetime.now()

        self.log(f"[STATUS CHECK] Starting periodic device status check at {now.strftime('%H:%M:%S')}")

        for device_id in self.device_names.keys():
            device_name = self.device_names.get(device_id, "unknown")
            device_type = getattr(self, 'device_types', {}).get(device_id, "Unknown")

            if device_type == "Hub":
                self.log(f"[STATUS CHECK] Skipping {device_name} - Hub devices don't need monitoring")
                continue

            # For LoRa devices (DoorSensor, THSensor, etc), poll API to check Hub connectivity
            # Don't use time-based detection - these devices only transmit on events
            self.log(f"[STATUS CHECK] Polling API for {device_name} Hub connectivity status")
            self.create_task(self.poll_device_status(device_id))

        self.log(f"[STATUS CHECK] Completed periodic device status check")

    def update_device_online_status(self, device_id, is_online, mark_sensor_unavailable=True):
        """Update device online/offline status and create sensor"""
        device_name = self.device_names.get(device_id, "unknown").replace(" ", "_").lower()
        device_type = getattr(self, 'device_types', {}).get(device_id, "Unknown")

        current_status = self.device_status.get(device_id, {}).get("online")
        if current_status == is_online:
            return

        self.device_status[device_id] = {
            "online": is_online,
            "last_update": datetime.now()
        }

        entity_id = f"binary_sensor.yolink_second_home_{device_id}_online"

        attributes = {
            "friendly_name": f"YoLink Second Home {device_name} Online",
            "device_class": "connectivity",
            "device_id": device_id,
            "last_seen": self.device_last_seen.get(device_id, "unknown"),
            "home_location": "second_home"
        }

        try:
            self.set_state(entity_id, state="on" if is_online else "off", attributes=attributes)

            if is_online:
                self.log(f"[DEVICE ONLINE] Device {device_name} (ID: {device_id}) is now online")
            else:
                self.log(f"[DEVICE OFFLINE] Device {device_name} (ID: {device_id}) has gone offline")

            # Only mark sensor entities unavailable if explicitly requested (not for LoRa devices)
            if not is_online and mark_sensor_unavailable:
                if device_type == "DoorSensor":
                    sensor_entity = f"binary_sensor.yolink_second_home_{device_id}_{device_name}"
                    if self.entity_exists(sensor_entity):
                        self.set_state(sensor_entity, state="unavailable")
                        self.log(f"Marked {sensor_entity} as unavailable due to offline status")

                elif device_type == "THSensor":
                    temp_entity = f"sensor.yolink_second_home_{device_id}_temperature"
                    humidity_entity = f"sensor.yolink_second_home_{device_id}_humidity"
                    battery_entity = f"sensor.yolink_second_home_{device_id}_battery"

                    for entity in [temp_entity, humidity_entity, battery_entity]:
                        if self.entity_exists(entity):
                            self.set_state(entity, state="unavailable")
                            self.log(f"Marked {entity} as unavailable due to offline status")

            if not is_online:
                self.send_offline_notification(device_id, device_name)

        except Exception as e:
            self.log(f"Error updating online status: {e}", level="ERROR")

    def send_offline_notification(self, device_id, device_name):
        """Send offline notification with rate limiting based on configuration"""
        if not self.offline_notifications_enabled:
            self.log(f"[NOTIFICATION DISABLED] Offline notifications disabled in config, "
                    f"skipping notification for {device_name} (ID: {device_id})")
            return

        now = datetime.now()
        last_notification_time = self.device_last_notification.get(device_id)

        if last_notification_time:
            seconds_since_last = (now - last_notification_time).total_seconds()
            if seconds_since_last < self.offline_notification_interval:
                remaining_time = self.offline_notification_interval - seconds_since_last
                next_notification_time = now + timedelta(seconds=remaining_time)

                hours_since = int(seconds_since_last // 3600)
                minutes_since = int((seconds_since_last % 3600) // 60)
                hours_remaining = int(remaining_time // 3600)
                minutes_remaining = int((remaining_time % 3600) // 60)

                self.log(f"[NOTIFICATION RATE LIMIT] Device: {device_name} (ID: {device_id})")
                self.log(f"  Last notification sent: {last_notification_time.strftime('%Y-%m-%d %H:%M:%S')} "
                        f"({hours_since}h {minutes_since}m ago)")
                self.log(f"  Next notification allowed: {next_notification_time.strftime('%Y-%m-%d %H:%M:%S')} "
                        f"(in {hours_remaining}h {minutes_remaining}m)")
                self.log(f"  Notification interval: {self.offline_notification_interval/3600:.1f} hours")
                return

        try:
            self.call_service("notify/notify",
                            title="YoLink Device Offline",
                            message=f"YoLink device {device_name} at second home is offline")

            self.device_last_notification[device_id] = now
            next_notification_time = now + timedelta(seconds=self.offline_notification_interval)

            self.log(f"[NOTIFICATION SENT] Device: {device_name} (ID: {device_id})")
            self.log(f"  Notification sent at: {now.strftime('%Y-%m-%d %H:%M:%S')}")
            self.log(f"  Next notification allowed: {next_notification_time.strftime('%Y-%m-%d %H:%M:%S')} "
                    f"({self.offline_notification_interval/3600:.1f} hours from now)")

        except Exception as e:
            self.log(f"[NOTIFICATION ERROR] Failed to send offline notification for {device_name} (ID: {device_id}): {e}", level="ERROR")

    def can_connect_mqtt(self):
        """Check if we can establish a new MQTT connection based on YoLink's rate limits"""
        now = datetime.now()

        while self.connection_timestamps and (now - self.connection_timestamps[0]).total_seconds() > self.rate_limit_window:
            self.connection_timestamps.popleft()

        if len(self.connection_timestamps) >= self.max_connections_per_window:
            oldest = self.connection_timestamps[0]
            seconds_until_slot = self.rate_limit_window - (now - oldest).total_seconds()
            self.log(f"MQTT rate limit reached (10/5min). Need to wait {seconds_until_slot:.1f} seconds", level="WARNING")
            return False

        if self.active_connections >= self.max_concurrent_connections:
            self.log(f"Maximum concurrent MQTT connections reached ({self.max_concurrent_connections})", level="WARNING")
            return False

        return True

    def calculate_backoff_time(self):
        """Calculate progressive backoff time respecting YoLink limits"""
        if not self.can_connect_mqtt():
            return self.rate_limit_window + 30

        backoff = min(300, 60 * (2 ** (self.mqtt_connection_attempts - 1)))
        return backoff

    def determine_connection_method(self):
        """Determine which connection method to use - WebSocket first, TCP fallback"""
        if not self.websocket_failed or (self.mqtt_connection_attempts <= 2):
            return ConnectionMethod.WEBSOCKET
        else:
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
            if self.mqtt_client:
                try:
                    self.mqtt_client.loop_stop()
                    self.mqtt_client.disconnect()
                    self.active_connections = max(0, self.active_connections - 1)
                except Exception:
                    pass

            connection_method = self.determine_connection_method()
            self.current_connection_method = connection_method

            self.connection_state = ConnectionState.CONNECTING

            if connection_method == ConnectionMethod.WEBSOCKET:
                self.mqtt_client = mqtt.Client(client_id=str(uuid.uuid4()), transport="websockets")
            else:
                self.mqtt_client = mqtt.Client(client_id=str(uuid.uuid4()))

            self.mqtt_client.username_pw_set(self.access_token)
            self.mqtt_client.on_connect = self.on_connect
            self.mqtt_client.on_message = self.on_message
            self.mqtt_client.on_disconnect = self.on_disconnect

            self.connection_timestamps.append(datetime.now())
            self.active_connections += 1

            if connection_method == ConnectionMethod.WEBSOCKET:
                self.log(f"Connecting to MQTT broker via WebSocket: {self.mqtt_host}:{self.mqtt_ws_port}")
                try:
                    self.mqtt_client.ws_set_options(path="/mqtt")
                    self.mqtt_client.connect_async(self.mqtt_host, self.mqtt_ws_port, 60)
                except Exception as e:
                    self.log(f"WebSocket connection failed with /mqtt path: {e}", level="WARNING")
                    try:
                        self.mqtt_client.ws_set_options(path="/")
                        self.mqtt_client.connect_async(self.mqtt_host, self.mqtt_ws_port, 60)
                    except Exception as e2:
                        self.log(f"WebSocket connection failed with / path: {e2}", level="ERROR")
                        raise e2
            else:
                self.log(f"Connecting to MQTT broker via TCP: {self.mqtt_host}:{self.mqtt_tcp_port}")
                self.mqtt_client.connect_async(self.mqtt_host, self.mqtt_tcp_port, 60)

            self.mqtt_client.loop_start()

            self.connection_timeout_handle = self.run_in(self.handle_connection_timeout, 30)

        except Exception as e:
            self.log(f"MQTT connection error: {e}", level="ERROR")
            self.active_connections = max(0, self.active_connections - 1)
            self.connection_state = ConnectionState.FAILED

            if self.current_connection_method == ConnectionMethod.WEBSOCKET:
                self.websocket_failed = True
                self.log("WebSocket connection failed, will try TCP next", level="WARNING")

            self.schedule_reconnect()

    def handle_connection_timeout(self, kwargs=None):
        """Handle connection timeout - force failover if WebSocket is hanging"""
        self.connection_timeout_handle = None

        if self.connection_state == ConnectionState.CONNECTING:
            self.log(f"Connection timeout after 30 seconds via {self.current_connection_method.value}", level="WARNING")

            if self.mqtt_client:
                try:
                    self.mqtt_client.loop_stop()
                    self.mqtt_client.disconnect()
                except Exception:
                    pass

            self.active_connections = max(0, self.active_connections - 1)
            self.connection_state = ConnectionState.FAILED

            if self.current_connection_method == ConnectionMethod.WEBSOCKET:
                self.websocket_failed = True
                self.log("WebSocket connection timeout, marking as failed and will try TCP", level="WARNING")

            self.schedule_reconnect()

    def on_connect(self, client, userdata, flags, rc):
        """Handle MQTT connection result"""
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

            if self.current_connection_method == ConnectionMethod.WEBSOCKET:
                self.websocket_failed = False

            if self.home_id:
                topic = f'yl-home/{self.home_id}/+/report'
                self.log(f"Subscribing to topic: {topic}")
                client.subscribe(topic)
            else:
                self.log("Home ID not available, retrying home ID fetch", level="WARNING")
                self.run_in(self.get_home_id_wrapper, 5)
        else:
            self.log(f"Failed to connect to MQTT Broker via {self.current_connection_method.value}, return code {rc}", level="ERROR")
            self.connection_state = ConnectionState.FAILED
            self.active_connections = max(0, self.active_connections - 1)

            if self.current_connection_method == ConnectionMethod.WEBSOCKET:
                self.websocket_failed = True
                self.log("WebSocket connection failed, will try TCP next", level="WARNING")

            self.schedule_reconnect()

    def on_disconnect(self, client, userdata, rc):
        """Handle MQTT disconnection"""
        self.log(f"MQTT disconnected from {self.current_connection_method.value if self.current_connection_method else 'unknown'} with result code: {rc}")
        self.connection_state = ConnectionState.DISCONNECTED
        self.active_connections = max(0, self.active_connections - 1)

        if rc != 0:
            if self.current_connection_method == ConnectionMethod.WEBSOCKET and rc in [7, 104]:
                self.websocket_failed = True
                self.log("WebSocket connection unstable, will prefer TCP for next connection", level="WARNING")

            self.schedule_reconnect()

    def schedule_reconnect(self):
        """Schedule MQTT reconnection with proper backoff"""
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

        self.mqtt_connection_attempts += 1
        backoff_time = self.calculate_backoff_time()

        if self.mqtt_connection_attempts > self.mqtt_max_retries:
            self.mqtt_connection_attempts = 0
            self.websocket_failed = False
            self.log(f"Maximum reconnection attempts reached. Resetting and will try again in {backoff_time} seconds", level="WARNING")
        else:
            self.log(f"Scheduling MQTT reconnection in {backoff_time:.1f} seconds (attempt {self.mqtt_connection_attempts})")

        self.connection_retry_handle = self.run_in(self.setup_mqtt_client, backoff_time)

    def convert_battery_level(self, yolink_battery):
        """Convert YoLink battery level (1-4) to percentage"""
        battery_map = {
            4: 100,
            3: 75,
            2: 50,
            1: 25
        }
        return battery_map.get(yolink_battery, 0)

    def on_message(self, client, userdata, msg):
        """Process incoming MQTT messages"""
        self.log(f"MQTT message received on topic: {msg.topic}")

        try:
            payload = json.loads(msg.payload.decode("utf-8"))

            self.log(f"Full MQTT payload: {json.dumps(payload, indent=2)}")

            event = payload.get("event")
            device_id = payload.get("deviceId")
            data = payload.get("data", {})

            if device_id:
                self.device_last_seen[device_id] = datetime.now()

                if event == "DeviceOffline" or event == "Device.StatusChange":
                    status = data.get("status") or data.get("state")
                    if status:
                        self.update_device_online_status(device_id, status != "offline", mark_sensor_unavailable=False)
                        self.log(f"Device {device_id} status changed to: {status}")

                # When we get any MQTT message, ensure device is marked online
                if device_id not in self.device_status or not self.device_status.get(device_id, {}).get("online"):
                    self.update_device_online_status(device_id, True, mark_sensor_unavailable=False)

            if event == "DoorSensor.Alert":
                state = data.get("state")
                self.log(f"DoorSensor event: device_id={device_id}, state={state}")
                self.publish_discovery_and_state(device_id, state)
                self.set_door_sensor_state(device_id, state)

                # Check for delivery event on door state change
                state_changed_at = data.get("stateChangedAt")
                if state_changed_at:
                    self.check_delivery_event(device_id, state_changed_at)
                else:
                    # Use current time if stateChangedAt not in MQTT payload
                    self.check_delivery_event(device_id, int(datetime.now().timestamp() * 1000))

            elif event == "THSensor.Report":
                self._process_thsensor_data(device_id, data)

            elif event == "THSensor.DataRecord":
                records = data.get("records", [])
                if records:
                    latest_record = records[-1]
                    self.log(f"Processing latest THSensor.DataRecord: device_id={device_id}, record={latest_record}")
                    self._process_thsensor_data(device_id, latest_record)
                else:
                    self.log(f"THSensor.DataRecord with no records: device_id={device_id}")

            else:
                self.log(f"Ignoring event: {event}: device_id={device_id}, payload={payload}")

        except json.JSONDecodeError as e:
            self.log(f"Invalid JSON in MQTT message: {e}", level="ERROR")
        except Exception as e:
            self.log(f"Error processing MQTT message: {e}", level="ERROR")

    def _process_thsensor_data(self, device_id, data):
        """Process temperature/humidity data from either Report or DataRecord events"""
        temp_c = data.get("temperature")
        temp_f = temp_c * 9 / 5 + 32 if temp_c is not None else None
        humidity = data.get("humidity")
        battery_raw = data.get("battery")
        battery = self.convert_battery_level(battery_raw) if battery_raw is not None else None

        self.log(
            f"THSensor data: device_id={device_id}, "
            f"temperature={temp_c}C/{temp_f}F, humidity={humidity}%, "
            f"battery={battery}% (raw: {battery_raw})"
        )

        self.publish_thsensor_state(device_id, temp_f, humidity, battery, data)
        self.set_thsensor_state(device_id, temp_f, humidity, battery, data)

    def publish_thsensor_state(self, device_id, temp_f, humidity, battery, data):
        """Publish Home Assistant MQTT discovery and state for temperature/humidity sensor"""
        device_name = self.device_names.get(device_id, "unknown").replace(" ", "_").lower()

        temp_topic_suffix = f"yolink_second_home_{device_id}_temperature"
        temp_discovery_topic = f"homeassistant/sensor/{temp_topic_suffix}/config"
        temp_state_topic = f"homeassistant/sensor/{temp_topic_suffix}/state"

        temp_discovery_payload = {
            "device_class": "temperature",
            "name": f"YoLink Second Home {device_name} Temperature",
            "state_topic": temp_state_topic,
            "unit_of_measurement": "°F",
            "value_template": "{{ value_json.temperature }}",
            "json_attributes_topic": temp_state_topic,
            "unique_id": temp_topic_suffix,
            "device": {
                "identifiers": [f"yolink_second_home_{device_id}"],
                "name": f"YoLink Second Home THSensor {device_name}",
                "model": "TH Sensor v1",
                "manufacturer": "YoLink"
            }
        }

        humidity_topic_suffix = f"yolink_second_home_{device_id}_humidity"
        humidity_discovery_topic = f"homeassistant/sensor/{humidity_topic_suffix}/config"
        humidity_state_topic = f"homeassistant/sensor/{humidity_topic_suffix}/state"

        humidity_discovery_payload = {
            "device_class": "humidity",
            "name": f"YoLink Second Home {device_name} Humidity",
            "state_topic": humidity_state_topic,
            "unit_of_measurement": "%",
            "value_template": "{{ value_json.humidity }}",
            "json_attributes_topic": humidity_state_topic,
            "unique_id": humidity_topic_suffix,
            "device": {
                "identifiers": [f"yolink_second_home_{device_id}"],
                "name": f"YoLink Second Home THSensor {device_name}",
                "model": "TH Sensor v1",
                "manufacturer": "YoLink"
            }
        }

        battery_topic_suffix = f"yolink_second_home_{device_id}_battery"
        battery_discovery_topic = f"homeassistant/sensor/{battery_topic_suffix}/config"
        battery_state_topic = f"homeassistant/sensor/{battery_topic_suffix}/state"

        battery_discovery_payload = {
            "device_class": "battery",
            "name": f"YoLink Second Home {device_name} Battery",
            "state_topic": battery_state_topic,
            "unit_of_measurement": "%",
            "value_template": "{{ value_json.battery }}",
            "json_attributes_topic": battery_state_topic,
            "unique_id": battery_topic_suffix,
            "device": {
                "identifiers": [f"yolink_second_home_{device_id}"],
                "name": f"YoLink Second Home THSensor {device_name}",
                "model": "TH Sensor v1",
                "manufacturer": "YoLink"
            }
        }

        temp_state_payload = {"temperature": temp_f, "battery": battery, "humidity": humidity}
        humidity_state_payload = {"humidity": humidity, "battery": battery, "temperature": temp_f}
        battery_state_payload = {"battery": battery, "temperature": temp_f, "humidity": humidity}

        try:
            self.call_service("mqtt/publish",
                            topic=temp_discovery_topic,
                            payload=json.dumps(temp_discovery_payload),
                            qos=0, retain=True)
            self.call_service("mqtt/publish",
                            topic=temp_state_topic,
                            payload=json.dumps(temp_state_payload),
                            qos=0, retain=True)

            self.call_service("mqtt/publish",
                            topic=humidity_discovery_topic,
                            payload=json.dumps(humidity_discovery_payload),
                            qos=0, retain=True)
            self.call_service("mqtt/publish",
                            topic=humidity_state_topic,
                            payload=json.dumps(humidity_state_payload),
                            qos=0, retain=True)

            self.call_service("mqtt/publish",
                            topic=battery_discovery_topic,
                            payload=json.dumps(battery_discovery_payload),
                            qos=0, retain=True)
            self.call_service("mqtt/publish",
                            topic=battery_state_topic,
                            payload=json.dumps(battery_state_payload),
                            qos=0, retain=True)

        except Exception as e:
            self.log(f"Error publishing THSensor to Home Assistant MQTT: {e}", level="ERROR")

    def set_thsensor_state(self, device_id, temp_f, humidity, battery, data):
        """Set Home Assistant entity states for temperature/humidity sensor"""
        device_name = self.device_names.get(device_id, "unknown").replace(" ", "_").lower()

        base_attributes = {
            "home_location": "second_home",
            "connection_method": self.current_connection_method.value if self.current_connection_method else "unknown",
            "device_id": device_id
        }

        temp_entity_id = f"sensor.yolink_second_home_{device_id}_temperature"
        temp_attributes = {
            **base_attributes,
            "friendly_name": f"YoLink Second Home {device_name} Temperature",
            "device_class": "temperature",
            "unit_of_measurement": "°F",
            "battery": battery,
            "humidity": humidity
        }

        humidity_entity_id = f"sensor.yolink_second_home_{device_id}_humidity"
        humidity_attributes = {
            **base_attributes,
            "friendly_name": f"YoLink Second Home {device_name} Humidity",
            "device_class": "humidity",
            "unit_of_measurement": "%",
            "battery": battery,
            "temperature": temp_f
        }

        battery_entity_id = f"sensor.yolink_second_home_{device_id}_battery"
        battery_attributes = {
            **base_attributes,
            "friendly_name": f"YoLink Second Home {device_name} Battery",
            "device_class": "battery",
            "unit_of_measurement": "%",
            "temperature": temp_f,
            "humidity": humidity
        }

        try:
            self.set_state(temp_entity_id, state=temp_f, attributes=temp_attributes)
            self.set_state(humidity_entity_id, state=humidity, attributes=humidity_attributes)
            self.set_state(battery_entity_id, state=battery, attributes=battery_attributes)

            self.log(f"Set THSensor states: temp={temp_f}°F, humidity={humidity}%, battery={battery}%")

        except Exception as e:
            self.log(f"Error setting THSensor entity states: {e}", level="ERROR")

    def publish_discovery_and_state(self, device_id, state):
        """Publish Home Assistant MQTT discovery and state"""
        device_name = self.device_names.get(device_id, "unknown").replace(" ", "_").lower()
        topic_suffix = f"yolink_second_home_{device_id}"
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
