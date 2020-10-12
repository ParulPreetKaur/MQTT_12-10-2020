import time
#import paho.mqtt.client as mqtt
import paho.mqtt.client as mqtt
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
    client.subscribe("directionalroadinfo/minoraccident")
# Callback Function on Receiving the Subscribed Topic/Message
def on_message( client, userdata, msg):
    # print the message received from the subscribed topic
    #print ( str(msg.payload) )
    print("Received message payload: {0}".format(str(msg.payload)))

def get_loc1():
     lane = 5
     print("Hello from a function")
     if (lane <= 10):
         print("parul")
         direction = "East to West"
     return (direction)


#output = get_loc()
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
#def publish_rsu():
    Data={}
    Data1={}
    Data['accident'] = "accident on lane 5:Switch to lane 72"
    Data['direction'] = "South to North"
    Data1['traffic'] = " heavy traffic at Highway847"
    Data1['directions'] = "North to South"
    Data_json_data = json.dumps(Data)
    Data_json_data1 = json.dumps(Data1)
    client.publish("client1/accident", Data_json_data)
    time.sleep(7)
    client.publish("client1/traffic", Data_json_data1)
    time.sleep(10)
    #time.sleep(10)
    #client.publish("client1", "traffic controller: at lane 417,drive acc to speed")
    #time.sleep(15)
    #lane = 5
    #print("Hello from a function")
    #if (lane <= 10):
    #print("parul")
    #direction = "East to West"
    #client.publish("client1", "East to West")
    #time.sleep(5)
#publish_rsu()



#publish_executed_command_message()
#connection.rollback()




    #time.sleep(15)

client.loop_stop()

client.disconnect()


#if __name__ == '__main__':
 #   output = get_loc1()
  #  print(output)
