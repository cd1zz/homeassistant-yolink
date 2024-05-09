# You need to create User Access Credentials (UAC) for the additional home in yolink
# <apps.yaml>
# yolink_second_home:
#  module: yolink_second_home
#  class: YoLinkDoorSensor
#  uaid: "ua_5...."
#  secret_key: "sec_v1_....."
#  api_url: "https://api.yosmart.com/open/yolink/v2/api"
#  api_token_url: "https://api.yosmart.com/open/yolink/token"
#  mqtt_host: "api.yosmart.com"
#  mqtt_port: 8003
#  token_refresh_interval: 3000

import json
import time
import paho.mqtt.client as mqtt
import aiohttp
import appdaemon.plugins.hass.hassapi as hass

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

        # Logging for Debugging
        self.log("Starting initialization...")

        # Schedule async tasks
        self.run_in(self.get_access_token, 0)
        self.run_in(self.get_home_id, 2)
        self.run_in(self.get_device_list, 4)

        # Initialize and connect MQTT client
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

        self.run_in(self.setup_mqtt_client, 5)

        self.run_every(self.refresh_access_token, "now", self.token_refresh_interval)

    async def get_access_token(self, kwargs=None):
        data = {
            "grant_type": "client_credentials",
            "client_id": self.uaid,
            "client_secret": self.secret_key
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(self.api_token_url, data=data) as response:
                if response.status == 200:
                    json_response = await response.json()
                    self.access_token = json_response.get("access_token")
                    #self.log(f"Access Token obtained: {self.access_token}")
                    self.log(f"Access Token obtained")
                else:
                    self.log("Failed to obtain access token")
                    raise Exception("Failed to obtain access token")

    async def refresh_access_token(self, kwargs):
        await self.get_access_token()
        self.mqtt_client.username_pw_set(self.access_token)

    async def get_home_id(self, kwargs=None):
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "method": "Home.getGeneralInfo",
            "time": int(time.time() * 1000)
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(self.api_url, headers=headers, json=payload) as response:
                if response.status == 200:
                    json_response = await response.json()
                    self.home_id = json_response.get("data").get("id")
                    self.log(f"Home ID obtained: {self.home_id}")
                else:
                    self.log("Failed to obtain Home ID")
                    raise Exception("Failed to obtain Home ID")

    async def get_device_list(self, kwargs=None):
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "method": "Home.getDeviceList",
            "time": int(time.time() * 1000)
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(self.api_url, headers=headers, json=payload) as response:
                if response.status == 200:
                    devices = (await response.json()).get("data", {}).get("devices", [])
                    for device in devices:
                        self.device_names[device["deviceId"]] = device["name"].replace(" ", "_").lower()
                    self.log("Device list fetched and names stored.")
                else:
                    self.log("Error occurred while fetching device list")

    def setup_mqtt_client(self, kwargs):
        if self.access_token is None:
            self.log("Cannot start MQTT client without an access token")
            return

        self.mqtt_client.username_pw_set(self.access_token)
        self.mqtt_client.connect(self.mqtt_host, self.mqtt_port, 60)
        self.mqtt_client.loop_start()
        self.log("MQTT client initialized and connected")

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.log("Connected to MQTT Broker!")
            client.subscribe(f'yl-home/{self.home_id}/#')
        else:
            self.log(f"Failed to connect, return code {rc}\n")

    def on_message(self, client, userdata, msg):
        self.log(f"Received `{msg.payload}` from `{msg.topic}` topic")
        payload = json.loads(msg.payload)
        event = payload.get("event")
        device_id = payload.get("deviceId")
        data = payload.get("data", {})

        if event == "DoorSensor.Alert":
            state = data.get("state")
            self.set_door_sensor_state(device_id, state)

    def set_door_sensor_state(self, device_id, state):
        device_name = self.device_names.get(device_id, "unknown").replace(" ", "_").lower()
        entity_id = f"binary_sensor.yolink_{device_id}_{device_name}"
        attributes = {
            "friendly_name": f"YoLink Door Sensor {device_name}",
            "device_class": "door"
        }
        self.set_state(entity_id, state=state, attributes=attributes)
        self.log(f"Sensor {entity_id} set to {state}")
