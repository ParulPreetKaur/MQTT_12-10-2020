import _thread
import time
import paho.mqtt.client as mqtt
import urllib.parse
import requests
from config import api_key
import json
import math
import csv
import random


cname = "client1"
connection_status_topic="sensor/connected/"+cname

def on_disconnect(client, userdata, flags, rc=0):
    m="DisConnected flags"+"result code "+str(rc)
    print(m)
    client.connected_flag=False

# Callback Function on Connection with MQTT Server
def on_connect( client, userdata, flags, rc):
 if rc == 0:
    print("Connected with Code :" +str(rc))
    # Subscribe Topic from here
    client.subscribe("emergency/#")
 else:
     print("Bad connection Returned code=", rc)
     client.bad_connection_flag = True
# Callback Function on Receiving the Subscribed Topic/Message
def on_message( client, userdata, msg):
    # print the message received from the subscribed topic
    print("Received message payload: {0}".format(str(msg.payload)))

mqtt.Client.connected_flag=False #create flags
mqtt.Client.bad_connection_flag=False #
mqtt.Client.retry_count=0 #

client = mqtt.Client(cname)    #create new instance
client.on_connect = on_connect
client.on_message = on_message
client.will_set(connection_status_topic,"Client1:Disconnected",0,True)
client.connect("192.168.56.1", 1883, 60)
client.loop_start()


time.sleep(1)
client.publish(connection_status_topic,"Client1:Connected",0,True)#use retain flag




def publishing_client1(threadName,delay):
    Data = {}
    Data1 = {}
    Data['accident'] = "accident on lane 2:Switch to lane 3"
    Data['direction'] = "North to South"
    Data1['traffic'] = " Congestion on lane 8"
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
    time.sleep(10)
    client.publish("client1/traffic", Data_json_data1)


def subscribing_client(threadName, delay):
    number = random.uniform(0, 1)
    if (number <= 0.25):
        print(number)
        client.subscribe("SouthtoNorth/weather/#")
        client.subscribe("SouthtoNorth/minoraccident1")
        client.subscribe("SouthtoNorth/minortraffic")
        with open(r'C:\Users\Dell_\MQTT\coordinates_car1.txt') as csv_file:
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
                    #print(Distance)
                    if (distance <= 2):
                        print("You are in Safe Zone")
                        client.subscribe("client/ZoneA")
                        time.sleep(20)
                        client.unsubscribe("client/ZoneA")

                    else:
                        print("You are in Critical Zone")
                        client.subscribe("client/ZoneB")
                        time.sleep(20)
                        client.unsubscribe("client/ZoneB")

                time.sleep(10)
                line_count += 1
            print(f'Processed {line_count} lines.')
        time.sleep(300)
        client.unsubscribe("SouthtoNorth/weather/#")
        client.unsubscribe("SouthtoNorth/minoraccident1")
        client.unsubscribe("SouthtoNorth/minortraffic")
    elif (number > 0.25 and number <= 0.5):
        #print(number)
        client.subscribe("NorthtoSouth/weather/#")
        client.subscribe("NorthtoSouth/minoraccident1")
        client.subscribe("NorthtoSouth/minortraffic")
        with open(r'C:\Users\Dell_\MQTT\coordinates_car1.txt') as csv_file:
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
                    #print(Distance)
                    if (distance <= 2):
                        print("You are in Safe Zone")
                        client.subscribe("client/ZoneA")
                        time.sleep(20)
                        client.unsubscribe("client/ZoneA")

                    else:
                        print("You are in Critical Zone")
                        client.subscribe("client/ZoneB")
                        time.sleep(20)
                        client.unsubscribe("client/ZoneB")

                time.sleep(10)
                line_count += 1
            print(f'Processed {line_count} lines.')
        time.sleep(300)
        client.unsubscribe("NorthtoSouth/minoraccident1")
        client.unsubscribe("NorthtoSouth/minortraffic")
        client.unsubscribe("NorthtoSouth/weather/#")
    elif (number > 0.5 and number <= 0.75):
        #print(number)
        client.subscribe("WesttoEast/weather/#")
        client.subscribe("WesttoEast/minoraccident1")
        client.subscribe("WesttoEast/minortraffic")
        with open(r'C:\Users\Dell_\MQTT\coordinates_car1.txt') as csv_file:
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
                    #print(Distance)
                    if (distance <= 2):
                        print("You are in Safe Zone")
                        client.subscribe("client/ZoneA")
                        time.sleep(20)
                        client.unsubscribe("client/ZoneA")

                    else:
                        print("You are in Critical Zone")
                        client.subscribe("client/ZoneB")
                        time.sleep(20)
                        client.unsubscribe("client/ZoneB")

                time.sleep(10)
                line_count += 1
            print(f'Processed {line_count} lines.')

        time.sleep(300)
        client.unsubscribe("WesttoEast/weather/#")
        client.unsubscribe("WesttoEast/minoraccident1")
        client.unsubscribe("WesttoEast/minortraffic")

    else:
        #print(number)
        client.subscribe("EasttoWest/weather/#")
        client.subscribe("EasttoWest/minoraccident1")
        client.subscribe("EasttoWest/minortraffic")
        with open(r'C:\Users\Dell_\MQTT\coordinates_car1.txt') as csv_file:
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
                    #print(Distance)
                    if (distance <= 2):
                        print("You are in Safe Zone")
                        client.subscribe("client/ZoneA")
                        time.sleep(20)
                        client.unsubscribe("client/ZoneA")

                    else:
                        print("You are in Critical Zone")
                        client.subscribe("client/ZoneB")
                        time.sleep(20)
                        client.unsubscribe("client/ZoneB")

                time.sleep(10)
                line_count += 1
            print(f'Processed {line_count} lines.')
        time.sleep(300)
        client.unsubscribe("EasttoWest/weather/#")
        client.unsubscribe("EasttoWest/minoraccident1")
        client.unsubscribe("EasttoWest/minortraffic")

try:
   _thread.start_new_thread( publishing_client1, ("PubThread-1", 2, ) )
   _thread.start_new_thread( subscribing_client, ("SubThread-2", 4, ) )
except:
   print ("Error: unable to start thread")

while 1:
   pass

client.loop_stop()
print("updating status and disconnecting")
client.publish(connection_status_topic,"Client 1 Disconnnected",0,True)
client.disconnect()


#if __name__ == '__main__':
 #   output = get_loc1()
  #  print(output)
