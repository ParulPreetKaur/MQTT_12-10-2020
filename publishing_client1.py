import time
import paho.mqtt.client as mqtt
import urllib.parse
import requests
from config import api_key
import json

#from database_connection import cursor, connection, psycopg2
#import psycopg2
#from database_connectionclose import *
# Callback Function on Connection with MQTT Server
def on_connect( client, userdata, flags, rc):
    print("Connected with Code :" +str(rc))
    # Subscribe Topic from here
    client.subscribe("home/#")
    #client.subscribe("client1/#")
    client.subscribe("weather/#")
    client.subscribe("roadinfo/#")
    client.subscribe("client1directionalroadinfo/minoraccident1")
    client.subscribe("client1/RSUinfo")
# Callback Function on Receiving the Subscribed Topic/Message
def on_message( client, userdata, msg):
    # print the message received from the subscribed topic
    #print ( str(msg.payload) )
    print("Received message payload: {0}".format(str(msg.payload)))
#print(output)
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect("192.168.56.1", 1883, 60)
client.loop_start()
time.sleep(1)

#def publish_executed_command_message():
    #client.publish("client1", "accident on lane 5 : Switch to lane 3")
    #time.sleep(5)
    #client.publish("client1", "heavy traffic at Highway417")
    #time.sleep(5)
    #client.publish("client1", "traffic controller at lane 417:drive acc to speed")
    #time.sleep(5)

while True:
    Data = {}
    Data1 = {}
    Data['accident'] = "accident on lane 15:Switch to lane 8562"
    Data['direction'] = "East to West"
    Data1['traffic'] = " heavy traffic at HighwaySS7"
    Data1['directions'] = "North to South"
    Data_json_data = json.dumps(Data)
    Data_json_data1 = json.dumps(Data1)
    endpoint = "https://maps.googleapis.com/maps/api/directions/json?"
    origin = '1177+Belanger+Avenue'
    destination = '7+Saxton+Private'
    nav_request = 'origin={} &destination={} &key={}'.format(origin, destination, api_key)
    request = endpoint + nav_request
    r = requests.get(request)
    decodedRes = r.text
        ###converting json to dictionary
    json_object = json.loads(decodedRes)
    direction = json_object["routes"][0]["bounds"]
    url = 'https://nominatim.openstreetmap.org/search/' + urllib.parse.quote("1177%20Belanger%20Avenue") + '?format=json'
    response = requests.get(url).json()
    cardirections = []
    carlatlong = []
    for key, value in direction.items():
       cardirections.append(key)
       carlatlong.append(value)
    #time.sleep(5)
    client.publish("client1/direction", 'client1' + ':' + cardirections[0] + ' to ' + cardirections[1])
    client.publish("client1/latlong", 'client3' + ':' + response[0]["lat"] + ':' + response[0]["lon"])
    time.sleep(10)
    client.publish("client1/accident", Data_json_data)
    time.sleep(15)
    client.publish("client1/traffic", Data_json_data1)
    time.sleep(10)



#publish_executed_command_message()
#connection.rollback()




    #time.sleep(15)

client.loop_stop()

client.disconnect()


#if __name__ == '__main__':
 #   output = get_loc1()
  #  print(output)
