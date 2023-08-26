from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder
import time as time
import datetime
import json
import adafruit_dht as dht
import board
import RPi.GPIO as GPIO
from bmp280 import BMP280
try:
    from smbus2 import SMBus
except ImportError:
        from smbus import SMBus

GPIO.setmode(GPIO.BCM)
GPIO.setup(17, GPIO.OUT)
dht_device = dht.DHT11(board.D4)
bus = SMBus(1)
bmp280 = BMP280(i2c_dev=bus)

# endpoint
ENDPOINT = ""
#client_id
CLIENT_ID = ""
# file paths
PATH_TO_CERT = ""
PATH_TO_KEY = ""
PATH_TO_ROOT = ""
# message
MESSAGE = ""
# topic
TOPIC = ""
# range
RANGE = 20

event_loop_group = io.EventLoopGroup(1)
host_resolver = io.DefaultHostResolver(event_loop_group)
client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
mqtt_connection = mqtt_connection_builder.mtls_from_path(
    endpoint = ENDPOINT,
    cert_filepath = PATH_TO_CERT,
    pri_key_filepath = PATH_TO_KEY,
    client_bootstrap = client_bootstrap,
    ca_filepath = PATH_TO_ROOT,
    client_id = CLIENT_ID,
    clean_session = False,
    keep_alive_secs = 6
)
print("Connecting to {} with client ID '{}'...".format(ENDPOINT, CLIENT_ID))
connect_future = mqtt_connection.connect()
connect_future.result()
print("Connected!")
print('Begin Publish')
while(True):
    GPIO.output(17, GPIO.HIGH)
    time.sleep(1)
    try:
        t = dht_device.temperature
        h = dht_device.humidity
        a = bmp280.get_altitude()
        p = bmp280.get_pressure()

        a = round(a,1)
        p = round(p,1)

        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")

        print("Temperature: {} \nHumidity: {} \nAltitude: {} \nPressure: {} \n".format(t,h,a,p))
        message = {"timestamp": dt_str, "temperature":t, "humidity":h, "altitude":a, "pressure":p}

        mqtt_connection.publish(topic=TOPIC, payload=json.dumps(message), qos=mqtt.QoS.AT_LEAST_ONCE)
        print("Published: '" + json.dumps(message) + "' to the topic: " + "'device/iotdata'")

    except RuntimeError as error:
        print(error.args[0])
        time.sleep(1)
        continue
    except Exception as error:
        dht_device.exit()
        raise error
    GPIO.output(17, GPIO.LOW)
    time.sleep(1)
print('Publish End')
disconnect_future = mqtt_connection.disconnect()
disconnect_future.result()

