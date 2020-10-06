import time
import paho.mqtt.client as mqtt
from queringsql_tables import get_roadinfo, get_weatherdetails, get_minoraccidents
from database_connectionclose import *
#from publishing_client1 import get_loc1
#from connected_client1 import get_loc
from database_connection import cursor, connection
from datetime import datetime
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
    #client.subscribe("roadinfo/intersectionwarning")
    #client.subscribe("roadinfo/workzonewarning")

# Callback Function on Receiving the Subscribed Topic/Message
def on_message( client, userdata, msg):
    # print the message received from the subscribed topic
    print("Received message payload: {0}".format(str(msg.payload)))


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("192.168.0.13", 1883, 60)
roadinfo_required = []
weatherinfo_required = []
client.loop_start()

time.sleep(1)
print("hello")
while True:
    weatherinfo_required = get_weatherdetails()
    Totalrows = len(weatherinfo_required)
    print(Totalrows)
    #location_of_vehicle = get_loc()
    #print(location_of_vehicle)
    #Totalrows_ma = len(minoraccident)
    #print(Totalrows_ma)

    for i in range(0,Totalrows):
        for weather_date in weatherinfo_required:
            df = pd.DataFrame({'my_timestamp': pd.date_range(weatherinfo_required[i][4], periods=1)})
            df['new_date'] = [d.date() for d in df['my_timestamp']]
            df['new_time'] = [d.time() for d in df['my_timestamp']]
            df[['h', 'm', 's']] = pd.DataFrame([(x.hour, x.minute, x.second) for x in df['new_time']])
        #print(df.h)
        #print (df.new_date)
        if str(df.new_date[0]) == '2020-09-18':
          if str(df.h[0]) == '8':
            client.publish("weather/forecast","Chances of Rain:" + str(weatherinfo_required[i][1]) + "%")
            client.publish("weather/temp", "Temperature:" + str(weatherinfo_required[i][0])+ " degree")
            client.publish("weather/humidity", "Humidity:" + str(weatherinfo_required[i][2]) +"%")
            client.publish("weather/visibility", "Visibility: 16.1 kms")
            time.sleep(1)
    #roadinfo_required = get_roadinfo()
    client.publish("roadinfo/workzonewarning", (roadinfo_required[0][1]))
    client.publish("roadinfo/intersectionwarning",(roadinfo_required[1][1]))
    #time.sleep(1)
    client.publish("roadinfo/traffic info", "Traffic light changing to red in 10 m")
    client.publish("roadinfo/traffic info", "Congestion on lane 8 and 9 on this route")
    minoraccident = get_minoraccidents()
    Totalrows_ma = len(minoraccident)
    #location = get_loc1()
    #print(location)
    print("rows in ma table are:  ", len(minoraccident))
    for s in range(0, Totalrows_ma):
        #if (minoraccident[s][1]!=location):
            client.publish("directionalroadinfo/minoraccident", str(minoraccident[s][2]))

    #time.sleep(15)
    connection_close()

client.loop_stop()
client.disconnect()