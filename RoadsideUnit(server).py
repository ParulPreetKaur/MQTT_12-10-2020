import time
import paho.mqtt.client as mqtt
from queringsql_tables import get_roadinfo, get_weatherdetails, get_minoraccidents, sensor_accident_Handler_from_Client1, sensor_traffic_Handler_from_Client1,sensor_clientlocation_from_Client1,get_clientdirectionfromdb,sensor_clientlatlong_from_Client1,get_latlonginfo,get_V2ISafety,get_minortraffic,get_emergencyinfo
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
    #client.subscribe("sensors/connected/#")
    client.subscribe("client1/accident")
    client.subscribe("client1/traffic")
    client.subscribe("client1/direction")
    client.subscribe("client1/latlong")
    client.subscribe("client2/latlong")
    #client.subscribe("sensors/connected/client1")
    #client.subscribe("client2/direction")
    #client.subscribe("SouthtoNorth/weather/forecast")
    #client.subscribe("NorthtoSouth/weather/forecast")

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
    emrgencyinfo=get_emergencyinfo()
    client.publish("emergency", str(emrgencyinfo[0][1]))
    weatherinfo_required = get_weatherdetails()
    Totalrows = len(weatherinfo_required)
    #print(Totalrows)
    for i in range(0,Totalrows):
        #for weather_date in weatherinfo_required:
            #df = pd.DataFrame({'my_timestamp': pd.date_range(weatherinfo_required[i][2], periods=1)})
            #df['new_date'] = [d.date() for d in df['my_timestamp']]
            #df['new_time'] = [d.time() for d in df['my_timestamp']]
            #df[['h', 'm', 's']] = pd.DataFrame([(x.hour, x.minute, x.second) for x in df['new_time']])

        #if str(df.new_date[0]) == '2020-11-19':
          #if str(df.h[0]) == '12':
        if(weatherinfo_required[i][10]=='North to South'):
            client.publish("NorthtoSouth/weather/forecast", "Chances of Rain:" + str(weatherinfo_required[i][1]) + "%")
            client.publish("NorthtoSouth/weather/temp", "Temperature:" + str(weatherinfo_required[i][0])+ " degree celsius")
            client.publish("NorthtoSouth/weather/feels_like", "Feels like:" + str(weatherinfo_required[i][7]) + " degree celsius")
            client.publish("NorthtoSouth/weather/humidity", "Humidity:" + str(weatherinfo_required[i][1]) +"%")
            client.publish("NorthtoSouth/weather/windspeed", "Wind Speed:" + str(weatherinfo_required[i][4]))
            client.publish("NorthtoSouth/weather/pressure", "Pressure:" + str(weatherinfo_required[i][5]) )
            client.publish("NorthtoSouth/weather/visibility", "Visibility:" + str(weatherinfo_required[i][6]))
            client.publish("NorthtoSouth/weather/desc", "Description:" + str(weatherinfo_required[i][8]))
            #time.sleep(1)
        if weatherinfo_required[i][10]=='South to North':
            client.publish("SouthtoNorth/weather/forecast", "Chances of Rain:" + str(weatherinfo_required[i][1]) + "%")
            client.publish("SouthtoNorth/weather/temp","Temperature:" + str(weatherinfo_required[i][0]) + " degree celsius")
            client.publish("SouthtoNorth/weather/feels_like","Feels like:" + str(weatherinfo_required[i][7]) + " degree celsius")
            client.publish("SouthtoNorth/weather/humidity", "Humidity:" + str(weatherinfo_required[i][1]) + "%")
            client.publish("SouthtoNorth/weather/windspeed", "Wind Speed:" + str(weatherinfo_required[i][4]))
            client.publish("SouthtoNorth/weather/pressure", "Pressure:" + str(weatherinfo_required[i][5]))
            client.publish("SouthtoNorth/weather/visibility", "Visibility:" + str(weatherinfo_required[i][6]))
            client.publish("SouthtoNorth/weather/desc", "Description:" + str(weatherinfo_required[i][8]))
        if weatherinfo_required[i][10]=='West to East':
            client.publish("WesttoEast/weather/forecast", "Chances of Rain:" + str(weatherinfo_required[i][1]) + "%")
            client.publish("WesttoEast/weather/temp","Temperature:" + str(weatherinfo_required[i][0]) + " degree celsius")
            client.publish("WesttoEast/weather/feels_like","Feels like:" + str(weatherinfo_required[i][7]) + " degree celsius")
            client.publish("WesttoEast/weather/humidity", "Humidity:" + str(weatherinfo_required[i][1]) + "%")
            client.publish("WesttoEast/weather/windspeed", "Wind Speed:" + str(weatherinfo_required[i][4]))
            client.publish("WesttoEast/weather/pressure", "Pressure:" + str(weatherinfo_required[i][5]))
            client.publish("WesttoEast/weather/visibility", "Visibility:" + str(weatherinfo_required[i][6]))
            client.publish("WesttoEast/weather/desc", "Description:" + str(weatherinfo_required[i][8]))
        else:
            client.publish("EasttoWest/weather/forecast", "Chances of Rain:" + str(weatherinfo_required[i][1]) + "%")
            client.publish("EasttoWest/weather/temp", "Temperature:" + str(weatherinfo_required[i][0]) + " degree celsius")
            client.publish("EasttoWest/weather/feels_like","Feels like:" + str(weatherinfo_required[i][7]) + " degree celsius")
            client.publish("EasttoWest/weather/humidity", "Humidity:" + str(weatherinfo_required[i][1]) + "%")
            client.publish("EasttoWest/weather/windspeed", "Wind Speed:" + str(weatherinfo_required[i][4]))
            client.publish("EasttoWest/weather/pressure", "Pressure:" + str(weatherinfo_required[i][5]))
            client.publish("EasttoWest/weather/visibility", "Visibility:" + str(weatherinfo_required[i][6]))
            client.publish("EasttoWest/weather/desc", "Description:" + str(weatherinfo_required[i][8]))
    roadinfo_required = get_roadinfo()
    V2Isafetyy=get_V2ISafety()
    ####SENDING INFO TO CONNECTED CLIENTS ACC TO DIRECTION
    minoraccident = get_minoraccidents()
    minortraffic = get_minortraffic()
    Totalrows_ma = len(minoraccident)
    location = get_clientdirectionfromdb()
    #client.publish("northeasttosouthwest/minoraccident1", str(minoraccident[0][1]))
    client.publish("NorthtoSouth/minoraccident1", str(minoraccident[1][1]))
    client.publish("WesttoEast/minoraccident1", "ALERT:" + str(minoraccident[2][1]))
    client.publish("SouthtoNorth/minoraccident1", "ALERT:" + str(minoraccident[0][1]))
    client.publish("EasttoWest/minoraccident1", str(minoraccident[3][1]))
    #client.publish("SouthwesttoNortheast/minoraccident1", "ALERT:" + str(minoraccident[3][1]))
    #client.publish("NorthtoSouth/minoraccident1", "ALERT:" + str(minoraccident[4][1]))
    #client.publish("EasttoWest/minoraccident1", "ALERT:" + str(minoraccident[5][1]))
    client.publish("SouthtoNorth/minortraffic", "ALERT:" + str(minortraffic[0][1]))
    client.publish("NorthtoSouth/minortraffic", "ALERT:" + str(minortraffic[1][1]))
    client.publish("EasttoWest/minortraffic", "ALERT:" + str(minortraffic[2][1]))
    client.publish("WesttoEast/minortraffic", "ALERT:" + str(minortraffic[3][1]))
    client.publish("client/ZoneA", "Safe Zone:" + str(V2Isafetyy[1][1]))
    time.sleep(5)
    client.publish("client/ZoneA", "Safe Zone:" + str(V2Isafetyy[2][1]))
    time.sleep(3)
    client.publish("client/ZoneA", "Safe Zone:" + str(V2Isafetyy[3][1]))
    time.sleep(3)
    client.publish("client/ZoneA", "Safe Zone:" + str(V2Isafetyy[4][1]))
    time.sleep(2)
    client.publish("client/ZoneA", "Safe Zone:" + str(V2Isafetyy[6][1]))
    time.sleep(1)
    client.publish("client/ZoneB", "Critical Zone:" + str(V2Isafetyy[5][1]))
    client.publish("client/ZoneB", "Critical Zone:" + str((roadinfo_required[0][1])))
    client.publish("client/ZoneB", "Critical Zone:" + str(V2Isafetyy[7][1]))
    #time.sleep(15)


    #connection_close()
client.loop_stop()
#client.disconnect()