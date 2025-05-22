# YoLink Second Home Integration

AppDaemon script for integrating YoLink devices from a second home into Home Assistant, designed to work alongside your existing YoLink integration.

## Why This Script?

- **Multiple Homes**: The official YoLink integration doesn't support multiple homes
- **Connection Reliability**: YoLink's TCP port 8003 has inconsistent connectivity issues, so this script uses WebSocket (port 8004) as the primary connection method with TCP fallback
- **Rate Limit Compliance**: Implements YoLink's strict API limits (5 concurrent, 10 connections per 5 minutes)

## Quick Setup

1. **Install** the script in `/config/apps/yolink/yolink_door_sensor.py`

2. **Configure** in `/config/apps/yolink/apps.yaml`:
   ```yaml
   yolink_second_home:
     module: yolink_door_sensor
     class: YoLinkDoorSensor
     uaid: "your_uaid_here"
     secret_key: "your_secret_key_here"
     api_url: "https://api.yosmart.com/open/yolink/v2/api"
     api_token_url: "https://api.yosmart.com/open/yolink/token"
     mqtt_host: api.yosmart.com
     mqtt_port: 8003
     mqtt_ws_port: 8004
     token_refresh_interval: 3600
   ```

3. **Get Credentials** from YoLink App: Menu → Settings → Account → Advanced Settings → User Access Credentials

## How It Works

### Connection Strategy
- **Primary**: WebSocket on port 8004 (more reliable than TCP)
- **Fallback**: TCP on port 8003 (if WebSocket fails)
- **Auto-Recovery**: 30-second timeout with automatic method switching

### Entity Creation
All devices are prefixed with `yolink_second_home_` to avoid conflicts:
- **Entity**: `binary_sensor.yolink_second_home_{device_id}_{name}`
- **Display**: `YoLink Second Home {Device Name} Door Sensor`

## Supported Devices

- **Door/Window Sensors**: Contact sensors with open/closed states
- **Attributes**: Battery level, signal strength, connection method

## Troubleshooting

### Expected Logs (Normal Operation)
```
INFO: Connecting to MQTT broker via WebSocket: api.yosmart.com:8004
INFO: Connected to MQTT Broker via websocket
INFO: Subscribing to topic: yl-home/{home_id}/+/report
```

### Connection Issues
```
WARNING: Connection timeout after 30 seconds via websocket
WARNING: WebSocket connection timeout, marking as failed and will try TCP
INFO: Connecting to MQTT broker via TCP: api.yosmart.com:8003
```
**This is normal** - the script automatically tries TCP if WebSocket fails.

### Rate Limiting (Expected)
```
WARNING: MQTT rate limit reached (10/5min). Need to wait 247.3 seconds
```
**Normal behavior** - YoLink limits connections, script will retry automatically.

### Common Problems

**No devices found**: Verify devices are in the correct YoLink "home" and UAC has access

**Authentication errors**: Check `uaid` and `secret_key` are correct UAC credentials (not CSID)

**Connection hangs**: The script handles this with timeouts and automatic failover

## Technical Details

### Rate Limiting
- **MQTT**: 5 concurrent connections, 10 per 5-minute window
- **API**: 100 requests per 5 minutes, 6 per device per minute
- **Implementation**: Sliding window tracking with progressive backoff

### Token Management
- **Auto-refresh**: Uses refresh tokens when available
- **Proactive**: Refreshes 5 minutes before expiration
- **Fallback**: Client credentials if refresh fails

## Requirements

- Home Assistant with AppDaemon
- YoLink account with UAC credentials
- MQTT integration enabled
- Outbound access to api.yosmart.com ports 8003/8004

## Why WebSocket First?

YoLink's TCP port 8003 has known reliability issues with timeouts and connection resets. WebSocket on port 8004 provides more stable connectivity, especially through firewalls and NAT devices. The script automatically falls back to TCP if WebSocket is unavailable.

---

**Note**: This script is designed for personal use. Please respect YoLink's API terms and rate limits.
