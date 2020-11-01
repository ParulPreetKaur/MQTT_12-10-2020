import time
import paho.mqtt.client as mqtt
import urllib.parse
import requests
from config import api_key
import json
import math
import csv
import random


#from database_connection import cursor, connection, psycopg2
#import psycopg2
#from database_connectionclose import *
# Callback Function on Connection with MQTT Server
def on_connect( client, userdata, flags, rc):
    print("Connected with Code :" +str(rc))
    # Subscribe Topic from here
    #client.subscribe("home/#")
    #client.subscribe("client1/#")
    client.subscribe("weather/#")
    #client.subscribe("roadinfo/#")
    #client.subscribe("client1directionalroadinfo/minoraccident1")
    #client.subscribe("client1/RSUinfo")
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



while True:

    Data = {}
    Data1 = {}
    Data['accident'] = "accident on lane 8:Switch to lane 59"
    Data['direction'] = "West to East"
    Data1['traffic'] = " Congestion on lane 568"
    Data1['directions'] = "South to North"
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
    with open(r'C:\Users\Dell_\Desktop\coordinates.txt') as csv_file:
        car1_csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for car1row in car1_csv_reader:
            if line_count == 0:
                print(f'Column names are {", ".join(car1row)}')
                line_count += 1
            else:
                R = 6373.0
                Distance = []
                lat1 = math.radians(float(car1row[0]))
                long1 = math.radians(float(car1row[1]))
                lat2 = math.radians(45.3529741)
                long2 = math.radians(-75.8082556718222)
                dlon = long2 - long1
                dlat = lat2 - lat1
                a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
                c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
                distance = R * c
                Distance.append(distance)
                print(Distance)
                if (distance <= 2):
                    client.subscribe("client/ZoneA")
                else:
                    client.subscribe("client/ZoneB")

            time.sleep(10)
            line_count += 1
        print(f'Processed {line_count} lines.')
    number = random.uniform(0, 1)
    print(number)
    if (number <= 0.25):
        print(number)
        client.subscribe("SouthtoNorth/minoraccident1")
        time.sleep(300)
    elif (number > 0.25 and number <= 0.5):
        client.subscribe("NorthtoSouth/minoraccident1")
        time.sleep(300)
    elif (number > 0.5 and number <= 0.75):
        client.subscribe("SouthwesttoNortheast/minoraccident1")
        time.sleep(300)
    else:
        client.subscribe("northeasttosouthwest/minoraccident1")
        time.sleep(300)

#publish_executed_command_message()
#connection.rollback()




    #time.sleep(15)

client.loop_stop()

client.disconnect()


#if __name__ == '__main__':
 #   output = get_loc1()
  #  print(output)
