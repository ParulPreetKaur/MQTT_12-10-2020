import time
import paho.mqtt.client as mqtt
import json
import requests
from config import api_key
import urllib.parse
import math
import random
import csv

cname="client2"
connection_status_topic="sensor/connected/"+cname
# Callback Function on Connection with MQTT Server
def on_connect( client, userdata, flags, rc):
  if rc == 0:
    print ("Connected with Code :" +str(rc))
    # Subscribe Topic from here
    client.subscribe("emergency/#")

  else:
     print("Bad connection Returned code=", rc)
     client.bad_connection_flag = True
# Callback Function on Receiving the Subscribed Topic/Message
def on_message( client, userdata, msg):
    # print the message received from the subscribed topic
    print ( str(msg.payload) )
mqtt.Client.connected_flag=False #create flags
mqtt.Client.bad_connection_flag=False #
mqtt.Client.retry_count=0 #

client = mqtt.Client("cname")
client.on_connect = on_connect
client.on_message = on_message
client.will_set(connection_status_topic,"Client2:Disconnected",0,True)

client.connect("192.168.56.1", 1883, 60)

#client.loop_forever()
client.loop_start()
time.sleep(1)
client.publish(connection_status_topic,"Client2:Connected",0,True)#use retain flag
while True:
        # def publish_rsu():
        Data_client2 = {}
        Data1_client2 = {}
        Data_client2['accident'] = "accident on lane 8:Switch to lane 9"
        Data_client2['direction'] = "West to East"
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
        number = random.uniform(0, 1)
        if (number <= 0.25):
                #print(number)
                client.subscribe("SouthtoNorth/weather/#")
                client.subscribe("SouthtoNorth/minoraccident1")
                client.subscribe("SouthtoNorth/minortraffic")
                with open(r'C:\Users\Dell_\MQTT\coordinates_car2.txt') as csv_file:
                        car2_csv_reader = csv.reader(csv_file, delimiter=',')
                        line_count = 0
                        for car2row in car2_csv_reader:
                                if line_count == 0:
                                        print(f'Column names are {", ".join(car2row)}')
                                        line_count += 1
                                else:
                                        R_car2 = 6373.0
                                        Distance_car2 = []
                                        lat1_car2 = math.radians(float(car2row[0]))
                                        long1_car2 = math.radians(float(car2row[1]))
                                        lat2_car2 = math.radians(45.3529741)
                                        long2_car2 = math.radians(-75.8082556718222)
                                        dlon_car2 = long2_car2 - long1_car2
                                        dlat_car2 = lat2_car2 - lat1_car2
                                        a_car2 = math.sin(dlat_car2 / 2) ** 2 + math.cos(lat1_car2) * math.cos(lat2_car2) * math.sin(
                                                dlon_car2 / 2) ** 2
                                        c_car2 = 2 * math.atan2(math.sqrt(a_car2), math.sqrt(1 - a_car2))
                                        distance_car2 = R_car2 * c_car2
                                        Distance_car2.append(distance_car2)
                                        print(Distance_car2)
                                        if (distance_car2 <= 2):
                                                print("You are in Zone A")
                                                client.subscribe("client/ZoneA")
                                                time.sleep(20)
                                                client.unsubscribe("client/ZoneA")

                                        else:
                                                print("You are in Zone B")
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
                with open(r'C:\Users\Dell_\MQTT\coordinates_car2.txt') as csv_file:
                        car2_csv_reader = csv.reader(csv_file, delimiter=',')
                        line_count = 0
                        for car2row in car2_csv_reader:
                                if line_count == 0:
                                        print(f'Column names are {", ".join(car2row)}')
                                        line_count += 1
                                else:
                                        R_car2 = 6373.0
                                        Distance_car2 = []
                                        lat1_car2 = math.radians(float(car2row[0]))
                                        long1_car2 = math.radians(float(car2row[1]))
                                        lat2_car2 = math.radians(45.3529741)
                                        long2_car2 = math.radians(-75.8082556718222)
                                        dlon_car2 = long2_car2 - long1_car2
                                        dlat_car2 = lat2_car2 - lat1_car2
                                        a_car2 = math.sin(dlat_car2 / 2) ** 2 + math.cos(lat1_car2) * math.cos(
                                                lat2_car2) * math.sin(
                                                dlon_car2 / 2) ** 2
                                        c_car2 = 2 * math.atan2(math.sqrt(a_car2), math.sqrt(1 - a_car2))
                                        distance_car2 = R_car2 * c_car2
                                        Distance_car2.append(distance_car2)
                                        print(Distance_car2)
                                        if (distance_car2 <= 2):
                                                print("You are in Zone A")
                                                client.subscribe("client/ZoneA")
                                                time.sleep(20)
                                                client.unsubscribe("client/ZoneA")

                                        else:
                                                print("You are in Zone B")
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
                with open(r'C:\Users\Dell_\MQTT\coordinates_car2.txt') as csv_file:
                        car2_csv_reader = csv.reader(csv_file, delimiter=',')
                        line_count = 0
                        for car2row in car2_csv_reader:
                                if line_count == 0:
                                        print(f'Column names are {", ".join(car2row)}')
                                        line_count += 1
                                else:
                                        R_car2 = 6373.0
                                        Distance_car2 = []
                                        lat1_car2 = math.radians(float(car2row[0]))
                                        long1_car2 = math.radians(float(car2row[1]))
                                        lat2_car2 = math.radians(45.3529741)
                                        long2_car2 = math.radians(-75.8082556718222)
                                        dlon_car2 = long2_car2 - long1_car2
                                        dlat_car2 = lat2_car2 - lat1_car2
                                        a_car2 = math.sin(dlat_car2 / 2) ** 2 + math.cos(lat1_car2) * math.cos(
                                                lat2_car2) * math.sin(
                                                dlon_car2 / 2) ** 2
                                        c_car2 = 2 * math.atan2(math.sqrt(a_car2), math.sqrt(1 - a_car2))
                                        distance_car2 = R_car2 * c_car2
                                        Distance_car2.append(distance_car2)
                                        print(Distance_car2)
                                        if (distance_car2 <= 2):
                                                print("You are in Zone A")
                                                client.subscribe("client/ZoneA")
                                                time.sleep(20)
                                                client.unsubscribe("client/ZoneA")

                                        else:
                                                print("You are in Zone B")
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
                with open(r'C:\Users\Dell_\MQTT\coordinates_car2.txt') as csv_file:
                        car2_csv_reader = csv.reader(csv_file, delimiter=',')
                        line_count = 0
                        for car2row in car2_csv_reader:
                                if line_count == 0:
                                        print(f'Column names are {", ".join(car2row)}')
                                        line_count += 1
                                else:
                                        R_car2 = 6373.0
                                        Distance_car2 = []
                                        lat1_car2 = math.radians(float(car2row[0]))
                                        long1_car2 = math.radians(float(car2row[1]))
                                        lat2_car2 = math.radians(45.3529741)
                                        long2_car2 = math.radians(-75.8082556718222)
                                        dlon_car2 = long2_car2 - long1_car2
                                        dlat_car2 = lat2_car2 - lat1_car2
                                        a_car2 = math.sin(dlat_car2 / 2) ** 2 + math.cos(lat1_car2) * math.cos(
                                                lat2_car2) * math.sin(
                                                dlon_car2 / 2) ** 2
                                        c_car2 = 2 * math.atan2(math.sqrt(a_car2), math.sqrt(1 - a_car2))
                                        distance_car2 = R_car2 * c_car2
                                        Distance_car2.append(distance_car2)
                                        print(Distance_car2)
                                        if (distance_car2 <= 2):
                                                print("You are in Zone A")
                                                client.subscribe("client/ZoneA")
                                                time.sleep(20)
                                                client.unsubscribe("client/ZoneA")

                                        else:
                                                print("You are in Zone B")
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

client.loop_stop()
client.disconnect()