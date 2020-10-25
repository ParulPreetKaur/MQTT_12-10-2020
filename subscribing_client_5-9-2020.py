import time
import paho.mqtt.client as mqtt
import json
import requests
from config import api_key
import urllib.parse

# Callback Function on Connection with MQTT Server
def on_connect( client, userdata, flags, rc):
    print ("Connected with Code :" +str(rc))
    # Subscribe Topic from here
    client.subscribe("Test/#")
    client.subscribe("weather/#")
    client.subscribe("roadinfo/#")
    client.subscribe("client2/RSUinfo")
    client.subscribe("client2directionalroadinfo/minoraccident1")

# Callback Function on Receiving the Subscribed Topic/Message
def on_message( client, userdata, msg):
    # print the message received from the subscribed topic
    print ( str(msg.payload) )

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("192.168.56.1", 1883, 60)

#client.loop_forever()
client.loop_start()
time.sleep(1)
while True:
        # def publish_rsu():
        Data_client2 = {}
        Data1_client2 = {}
        Data_client2['accident'] = "Road blockage yotoday"
        Data_client2['direction'] = "East to West"
        Data1_client2['traffic'] = " Avoid highway58"
        Data1_client2['directions'] = "West to East"
        Data_json_data_client2 = json.dumps(Data_client2)
        Data_json_data1_client2 = json.dumps(Data1_client2)
        endpoint2 = "https://maps.googleapis.com/maps/api/directions/json?"
        origin2 = '1177+Belanger+Avenue'
        destination2 = 'South+keys'
        nav_request2 = 'origin={} &destination={} &key={}'.format(origin2, destination2, api_key)
        request2 = endpoint2 + nav_request2
        r2 = requests.get(request2)
        decodedRes2 = r2.text
        ###converting json to dictionary
        json_object2 = json.loads(decodedRes2)
        direction2 = json_object2["routes"][0]["bounds"]
        car2directions = []
        car2latlong = []
        url = 'https://nominatim.openstreetmap.org/search/' + urllib.parse.quote("7%20Saxton%20Private") + '?format=json'
        response = requests.get(url).json()
        for key, value in direction2.items():
            car2directions.append(key)
            car2latlong.append(value)
        #client.publish("client1/direction", 'client2' + ':' + car2directions[0] + ' to ' + car2directions[1])
        #client.publish("client2/latlong", 'client2' + ':' + response[0]["lat"] + ':' + response[0]["lon"])
        time.sleep(10)
        client.publish("client1/accident", Data_json_data_client2)
        time.sleep(10)
        client.publish("client1/traffic", Data_json_data1_client2)
        time.sleep(10)

client.loop_stop()
client.disconnect()