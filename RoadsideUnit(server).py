import time
import paho.mqtt.client as mqtt
from queringsql_tables import get_roadinfo, get_weatherdetails, get_minoraccidents, sensor_accident_Handler_from_Client1, sensor_traffic_Handler_from_Client1,sensor_clientlocation_from_Client1,get_clientdirectionfromdb,sensor_clientlatlong_from_Client1,get_latlonginfo
from database_connectionclose import *
#from publishing_client1 import find_loc
#from connected_client1 import find_loc
from database_connection import cursor, connection
from datetime import datetime
import math
#from queringsql_tables import *
import pandas as pd

# datetime object containing current date and time

now = datetime.now()
current_date = now.date()
current_hour = now.hour
print("current_hour =", current_hour)
print("current_date =", current_date)
# Callback Function on Connection with MQTT Server
def on_connect( client, userdata, flags, rc):
    print ("Connected with Code :" +str(rc))
    # Subscribe Topic from here
    client.subscribe("client1/accident")
    client.subscribe("client1/traffic")
    client.subscribe("client1/direction")
    client.subscribe("client1/latlong")
    client.subscribe("client2/latlong")
    #client.subscribe("client2/direction")
    #client.subscribe("roadinfo/intersectionwarning")
    #client.subscribe("roadinfo/workzonewarning")

# Callback Function on Receiving the Subscribed Topic/Message
def on_message( client, userdata, msg):
    # print the message received from the subscribed topic
    #print(msg.payload)
    #print(msg.topic)
    print("Received message payload: {0}".format(str(msg.payload)))
    message=msg.payload.decode("utf-8")
    #print(message)

    if (msg.topic =="client1/accident"):
     sensor_accident_Handler_from_Client1(msg.topic, message)
    elif (msg.topic =="client1/traffic"):
        sensor_traffic_Handler_from_Client1(msg.topic, message)
    elif (msg.topic =="client1/latlong"):
         sensor_clientlatlong_from_Client1(msg.topic, message)
    elif(msg.topic == "client2/latlong"):
        sensor_clientlatlong_from_Client1(msg.topic, message)
    else:
       sensor_clientlocation_from_Client1(msg.topic, message)
    #return (message)

    #if(msg.payload=="East to West"):
        #location="East to West"
        #print(location)
        #print("direction from client 1")



client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("192.168.56.1", 1883, 60)
roadinfo_required = []
weatherinfo_required = []
client.loop_start()

time.sleep(1)
#print("hello")
while True:
    weatherinfo_required = get_weatherdetails()
    Totalrows = len(weatherinfo_required)
    print(Totalrows)
    for i in range(0,Totalrows):
        for weather_date in weatherinfo_required:
            df = pd.DataFrame({'my_timestamp': pd.date_range(weatherinfo_required[i][4], periods=1)})
            df['new_date'] = [d.date() for d in df['my_timestamp']]
            df['new_time'] = [d.time() for d in df['my_timestamp']]
            df[['h', 'm', 's']] = pd.DataFrame([(x.hour, x.minute, x.second) for x in df['new_time']])

        if str(df.new_date[0]) == '2020-10-11':
          if str(df.h[0]) == '14':
            client.publish("weather/forecast","Chances of Rain:" + str(weatherinfo_required[i][1]) + "%")
            client.publish("weather/temp", "Temperature:" + str(weatherinfo_required[i][0])+ " degree")
            client.publish("weather/humidity", "Humidity:" + str(weatherinfo_required[i][2]) +"%")
            client.publish("weather/visibility", "Visibility: 16.1 kms")
            time.sleep(1)
    roadinfo_required = get_roadinfo()
    client.publish("roadinfo/workzonewarning", (roadinfo_required[0][1]))
    #client.publish("roadinfo/intersectionwarning",(roadinfo_required[1][1]))
    time.sleep(1)
    #client.publish("roadinfo/traffic info", "Traffic light changing to red in 10 m")
    #client.publish("roadinfo/traffic info", "Congestion on lane 8 and 9 on this route")

    ####SENDING INFO TO CONNECTED CLIENTS ACC TO DIRECTION
    minoraccident = get_minoraccidents()
    Totalrows_ma = len(minoraccident)
    location = get_clientdirectionfromdb()
    print(location[0][1])
    print("rows in ma table are:  ", len(minoraccident))
    #for s in range(0, Totalrows_ma):
     #  if(location[0][0]=='client1'and minoraccident[0][0]==location[0][1]):
      #        print("yo baby")
    client.publish("client1directionalroadinfo/minoraccident1", str(minoraccident[0][1]))
    #   if(location[2][0]=='client3'and minoraccident[1][0]==location[2][1]):
    client.publish("client2directionalroadinfo/minoraccident1", str(minoraccident[0][1]))
    client.publish("client3directionalroadinfo/minoraccident1", str(minoraccident[2][1]))
    #time.sleep(15)
    ##FETCHING THE LATLONG INFO FROM DB AND FINDING DISTANCE BETWEEN CLIENT AND RSU TO DECIDE INFO TO BE SENT ACC TO DISTANCE(ZONE)
    latlonginfo=get_latlonginfo()
    tableentries=len(latlonginfo)
    print(tableentries)
    Distance=[]
    client_info=[]
    R = 6373.0
    for info in range(0, tableentries):
      if(latlonginfo[info][0]!="RSU"):
        lat1 = math.radians(latlonginfo[info][1])
        long1 = math.radians(latlonginfo[info][2])
        lat2 = math.radians(45.3529741)
        long2 = math.radians(-75.8082556718222)
        dlon = long2 - long1
        dlat = lat2 - lat1
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = R * c
        Distance.append(distance)
        client_info.append(latlonginfo[info][0])
    #print(Distance)
    resultant={}
    #print(client_info)
    print(latlonginfo[info][0]+":"+str(distance))
    res = dict(zip(client_info, Distance))
    print(str(res))
    for i, j in res.items():
        if(i=='client1' and j<=2):
            client.publish("client1/RSUinfo", "you are on green yellow")
        elif(i=='client2' and j<=2):
            print("hello")
            client.publish("client2/RSUinfo", "you are on green yellow ")
    connection_close()

client.loop_stop()
client.disconnect()