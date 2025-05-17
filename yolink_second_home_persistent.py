import uuid
import json
import time
from datetime import datetime, timedelta
import aiohttp
import paho.mqtt.client as mqtt
import appdaemon.plugins.hass.hassapi as hass
from collections import deque

class YoLinkDoorSensor(hass.Hass):
    def initialize(self):
        self.uaid = self.args["uaid"]
        self.secret_key = self.args["secret_key"]
        self.api_url = self.args["api_url"]
        self.api_token_url = self.args["api_token_url"]
        self.mqtt_host = self.args["mqtt_host"]
        self.mqtt_port = self.args.get("mqtt_port", 8003)
        self.token_refresh_interval = self.args.get("token_refresh_interval", 3000)
        self.access_token = None
        self.home_id = None
        self.device_names = {}
        self.mqtt_client = None
        
        # Rate limiting configuration
        self.max_concurrent_connections = 5  # Maximum allowed concurrent connections
        self.max_connections_per_window = 10  # Maximum connections in time window
        self.rate_limit_window = 5 * 60  # 5 minutes in seconds
        self.connection_timestamps = deque(maxlen=self.max_connections_per_window)
        self.mqtt_connected = False
        self.mqtt_reconnect_interval = 60  # Time between reconnection attempts in seconds
        self.connection_retry_handle = None
        self.mqtt_connection_attempts = 0
        self.mqtt_max_retries = 5
        self.mqtt_reconnect_backoff = 1.5  # Exponential backoff multiplier

        self.log("Starting initialization...")

        self.run_in(self.get_access_token, 0)
        self.run_in(self.get_home_id, 2)
        self.run_in(self.get_device_list, 4)
        self.run_in(self.setup_mqtt_client, 5)
        self.run_every(self.refresh_access_token, "now", self.token_refresh_interval)
        
    async def get_access_token(self, kwargs=None):
        data = {
            "grant_type": "client_credentials",
            "client_id": self.uaid,
            "client_secret": self.secret_key
        }
        self.log(f"Requesting access token from: {self.api_token_url}")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.api_token_url, json=data) as response:
                    self.log(f"Access token response status: {response.status}")
                    resp_text = await response.text()
                    if response.status == 200:
                        json_response = json.loads(resp_text)
                        self.access_token = json_response.get("access_token")
                        self.log("Access Token obtained.")
                    else:
                        raise Exception("Failed to obtain access token")
        except Exception as e:
            self.log(f"Access token error: {e}")

    async def refresh_access_token(self, kwargs):
        self.log("Refreshing access token...")
        await self.get_access_token()
        if self.mqtt_client and self.mqtt_connected:
            self.log("Setting new access token on MQTT client")
            self.mqtt_client.username_pw_set(self.access_token)

    async def get_home_id(self, kwargs=None):
        if not self.access_token:
            self.log("No access token available, skipping home ID request")
            return
            
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "method": "Home.getGeneralInfo",
            "time": int(time.time() * 1000)
        }
        self.log(f"Fetching Home ID from: {self.api_url}")
        self.log(f"Payload: {json.dumps(payload)}")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.api_url, headers=headers, json=payload) as response:
                    self.log(f"Home ID response status: {response.status}")
                    resp_text = await response.text()
                    self.log(f"Home ID response body: {resp_text}")
                    if response.status == 200:
                        json_response = json.loads(resp_text)
                        self.home_id = json_response.get("data", {}).get("id")
                        self.log(f"Home ID obtained: {self.home_id}")
                    else:
                        raise Exception("Failed to obtain Home ID")
        except Exception as e:
            self.log(f"Home ID error: {e}")

    async def get_device_list(self, kwargs=None):
        if not self.access_token:
            self.log("No access token available, skipping device list request")
            return
            
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "method": "Home.getDeviceList",
            "time": int(time.time() * 1000)
        }
        self.log(f"Fetching device list from: {self.api_url}")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.api_url, headers=headers, json=payload) as response:
                    self.log(f"Device list response status: {response.status}")
                    resp_text = await response.text()
                    if response.status == 200:
                        devices = json.loads(resp_text).get("data", {}).get("devices", [])
                        for device in devices:
                            self.device_names[device["deviceId"]] = device["name"].replace(" ", "_").lower()
                        self.log(f"Device list fetched, found {len(devices)} devices")
                    else:
                        raise Exception("Failed to fetch device list")
        except Exception as e:
            self.log(f"Device list error: {e}")

    def can_connect_mqtt(self):
        """Check if we can establish a new MQTT connection based on rate limits"""
        now = datetime.now()
        
        # Remove expired timestamps that are outside the time window
        while self.connection_timestamps and (now - self.connection_timestamps[0]).total_seconds() > self.rate_limit_window:
            self.connection_timestamps.popleft()
            
        # Check if we've hit the connection limit in the time window
        if len(self.connection_timestamps) >= self.max_connections_per_window:
            oldest = self.connection_timestamps[0]
            seconds_until_slot = self.rate_limit_window - (now - oldest).total_seconds()
            self.log(f"Rate limit reached. Need to wait {seconds_until_slot:.1f} seconds before next connection attempt")
            return False
            
        return True

    def setup_mqtt_client(self, kwargs):
        self.log("Initializing MQTT client...")
        if not self.access_token:
            self.log("Cannot start MQTT client without an access token")
            self.run_in(self.setup_mqtt_client, 10)  # Retry in 10 seconds
            return
            
        if not self.can_connect_mqtt():
            # Schedule retry after some time when a connection slot should be available
            next_attempt_time = max(30, self.rate_limit_window / self.max_connections_per_window)
            self.log(f"Rate limited, retry MQTT setup in {next_attempt_time} seconds")
            self.run_in(self.setup_mqtt_client, next_attempt_time)
            return

        try:
            # Clean up any existing client
            if self.mqtt_client:
                try:
                    self.mqtt_client.loop_stop()
                    self.mqtt_client.disconnect()
                except:
                    pass
                    
            self.mqtt_client = mqtt.Client(client_id=str(uuid.uuid4()))
            self.mqtt_client.username_pw_set(self.access_token)
            self.mqtt_client.on_connect = self.on_connect
            self.mqtt_client.on_message = self.on_message
            self.mqtt_client.on_disconnect = self.on_disconnect

            # Add the current time to our connection attempt history
            self.connection_timestamps.append(datetime.now())
            
            self.log(f"Connecting to MQTT broker: {self.mqtt_host}:{self.mqtt_port}")
            self.mqtt_client.connect_async(self.mqtt_host, self.mqtt_port, 60)
            self.mqtt_client.loop_start()
            self.log("MQTT client initialized and connection requested")
        except Exception as e:
            self.log(f"MQTT connection error: {e}")
            self.schedule_reconnect()

    def on_connect(self, client, userdata, flags, rc):
        self.log(f"MQTT on_connect: rc={rc}")
        if rc == 0:
            self.log("Connected to MQTT Broker")
            self.mqtt_connected = True
            self.mqtt_connection_attempts = 0  # Reset counter on successful connection
            
            if self.home_id:
                topic = f'yl-home/{self.home_id}/+/report'
                self.log(f"Subscribing to topic: {topic}")
                client.subscribe(topic)
            else:
                self.log("Home ID not available, can't subscribe to MQTT topic")
                self.run_in(self.get_home_id, 5)  # Try to get home_id again
        else:
            self.log(f"Failed to connect to MQTT Broker, return code {rc}")
            self.mqtt_connected = False
            self.schedule_reconnect()

    def on_disconnect(self, client, userdata, rc):
        self.log(f"MQTT disconnected with result code: {rc}")
        self.mqtt_connected = False
        
        # Only try to reconnect automatically for unexpected disconnects (non-zero rc)
        if rc != 0:
            self.schedule_reconnect()

    def schedule_reconnect(self):
        # Cancel any existing reconnect attempts
        if self.connection_retry_handle:
            try:
                self.cancel_timer(self.connection_retry_handle)
            except:
                pass
        
        # Apply exponential backoff
        self.mqtt_connection_attempts += 1
        if self.mqtt_connection_attempts > self.mqtt_max_retries:
            backoff_time = 300  # Cap at 5 minutes
            self.log(f"Maximum reconnection attempts reached. Will try again in {backoff_time} seconds.")
        else:
            backoff_time = min(300, self.mqtt_reconnect_interval * (self.mqtt_reconnect_backoff ** (self.mqtt_connection_attempts - 1)))
            self.log(f"Scheduling MQTT reconnection in {backoff_time:.1f} seconds (attempt {self.mqtt_connection_attempts})")
        
        # Schedule reconnection
        self.connection_retry_handle = self.run_in(self.setup_mqtt_client, backoff_time)

    def on_message(self, client, userdata, msg):
        self.log(f"MQTT message received on topic: {msg.topic}")
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            event = payload.get("event")
            device_id = payload.get("deviceId")
            state = payload.get("data", {}).get("state")

            if event == "DoorSensor.Alert":
                self.log(f"DoorSensor event detected: device_id={device_id}, state={state}")
                self.publish_discovery_and_state(device_id, state)
                self.set_door_sensor_state(device_id, state)
            else:
                self.log(f"Ignoring non-door event: {event}")
        except Exception as e:
            self.log(f"Error processing MQTT message: {e}")

    def publish_discovery_and_state(self, device_id, state):
        device_name = self.device_names.get(device_id, "unknown").replace(" ", "_").lower()
        topic_suffix = f"{device_id}"
        discovery_topic = f"homeassistant/binary_sensor/{topic_suffix}/config"
        state_topic = f"homeassistant/binary_sensor/{topic_suffix}/state" 

        discovery_payload = {
            "device_class": "door",
            "name": f"{device_name} Door Sensor",
            "state_topic": state_topic,
            "payload_on": "open",
            "payload_off": "closed",
            "value_template": "{{ value_json.state }}",
            "json_attributes_topic": state_topic,
            "unique_id": topic_suffix,
            "device": {
                "identifiers": [device_id],
                "name": "YoLink Door Sensor",
                "model": "Door Sensor v1",
                "manufacturer": "YoLink"
            }
        }

        state_payload = {
            "state": state
        }

        self.log(f"Publishing discovery to: {discovery_topic}")
        self.call_service("mqtt/publish", topic=discovery_topic, payload=json.dumps(discovery_payload), qos=0, retain=True)

        self.log(f"Publishing state to: {state_topic}")
        self.call_service("mqtt/publish", topic=state_topic, payload=json.dumps(state_payload), qos=0, retain=True)

    def set_door_sensor_state(self, device_id, state):
        device_name = self.device_names.get(device_id, "unknown").replace(" ", "_").lower()
        entity_id = f"binary_sensor.yolink_{device_id}_{device_name}"
        attributes = {
            "friendly_name": f"YoLink Door Sensor {device_name}",
            "device_class": "door"
        }
        self.log(f"Setting state: {entity_id} = {state} with attributes: {attributes}")
        self.set_state(entity_id, state=state, attributes=attributes)
