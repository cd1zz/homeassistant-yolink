# Uses paho mqtt with "retain" flag to persist server reboots

import appdaemon.plugins.hass.hassapi as hass
import requests
import time
import json
import paho.mqtt.client as mqtt
import time

class YoLinkDoorSensor(hass.Hass):
    def initialize(self):
        self.log("Starting initialization...")
        
        # Configured in apps.yaml
        self.uaid = self.args["uaid"]
        self.secret_key = self.args["secret_key"]
        self.api_url = self.args["api_url"]
        self.api_token_url = self.args["api_token_url"]
        self.mqtt_host = self.args["mqtt_host"]
        self.mqtt_port = self.args.get("mqtt_port")
        self.token_refresh_interval = self.args.get("token_refresh_interval")

        self.access_token = self.get_access_token()
        self.get_home_id()
        self.device_names = {}
        self.get_device_list()
        self.setup_mqtt_client()

    def get_access_token(self):
        data = {
            "grant_type": "client_credentials",
            "client_id": self.uaid,
            "client_secret": self.secret_key
        }
        response = requests.post(self.api_token_url, json=data)
        if response.status_code == 200:
            json_response = response.json()
            self.log(f"Access Token obtained")
            return json_response["access_token"]
        else:
            self.log(f"Failed to obtain access token, Status: {response.status_code}, Body: {response.text}")
            return None

    def get_home_id(self):
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "method": "Home.getGeneralInfo",
            "time": int(time.time() * 1000)
        }

        response = requests.post(self.api_url, headers=headers, json=payload)
        if response.status_code == 200:
            json_response = response.json()
            self.home_id = json_response.get("data", {}).get("id")
            self.log(f"Home ID obtained: {self.home_id}")
        else:
            self.log("Failed to obtain Home ID")
            raise Exception("Failed to obtain Home ID")

    def get_device_list(self):
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "method": "Home.getDeviceList",
            "time": int(time.time() * 1000)
        }

        response = requests.post(self.api_url, headers=headers, json=payload)
        if response.status_code == 200:
            json_response = response.json()
            devices = json_response.get("data", {}).get("devices", [])
            for device in devices:
                self.device_names[device["deviceId"]] = device["name"].replace(" ", "_").lower()
            self.log("Device list fetched and names stored.")
        else:
            self.log("Error occurred while fetching device list")
            raise Exception("Error occurred while fetching device list")


    def setup_mqtt_client(self):

        self.mqtt_client = mqtt.Client()
        self.mqtt_client.username_pw_set(self.access_token)
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

        try:
            self.mqtt_client.connect(self.mqtt_host, self.mqtt_port, 60)
            self.mqtt_client.loop_start()
        except Exception as e:
            self.log(f"Error while connecting: {e}. Sleeping 15 minutes.")
            time.sleep(900)

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.log(f"Connected to MQTT Broker on {self.mqtt_host}:{self.mqtt_port}")
            client.subscribe(f'yl-home/{self.home_id}/#')
        else:
            self.log(f"Failed to connect, return code {rc}")

    def on_message(self, client, userdata, msg):
        self.log(f"Received `{msg.payload.decode('utf-8')}` from `{msg.topic}` topic")
        try:
            message = json.loads(msg.payload.decode('utf-8'))
            if message['event'] == 'DoorSensor.Alert':
                device_id = message.get("deviceId", "")
                device_name = self.device_names.get(device_id)
                device_state = message.get("data", {}).get("state")

                # MQTT topics
                topic_suffix = f"{device_id}"
                discovery_topic = f"homeassistant/binary_sensor/{topic_suffix}/config"
                state_topic = f"homeassistant/binary_sensor/{topic_suffix}/state"

                # MQTT Discovery payload for Home Assistant
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
                self.publish_mqtt_message(discovery_topic, json.dumps(discovery_payload), only_if_new=True)
                self.log(f"Discovery published to {discovery_topic}")

                # Publish the actual sensor state
                state_payload = {
                    "state": device_state
                }
                self.publish_mqtt_message(state_topic, json.dumps(state_payload))
                self.log(f"State published to {state_topic}")
            else:
                self.log(f"Unrecognized yolink event type: {message['event']}")

        except json.JSONDecodeError as e:
            self.log(f"Error decoding JSON: {e}")
        except Exception as e:
            self.log(f"Unhandled error: {e}")

    def publish_mqtt_message(self, topic, payload, only_if_new=False):
        self.call_service("mqtt/publish", topic=topic, payload=payload, qos=0, retain=True)
