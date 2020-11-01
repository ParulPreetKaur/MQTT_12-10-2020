import csv
import math
import time
import random


import time
import paho.mqtt.client as mqtt
import urllib.parse
import requests
from config import api_key
import json

def on_connect( client, userdata, flags, rc):
    print("Connected with Code :" +str(rc))
def on_message( client, userdata, msg):
    print("Received message payload: {0}".format(str(msg.payload)))




client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect("192.168.56.1", 1883, 60)
client.loop_start()


while True:
 number= random.uniform(0, 1)
 if(number<=0.25):
    print(number)
    client.subscribe("SouthtoNorth/minoraccident1")
    time.sleep(300)
 elif(number>0.25 and number<=0.5):
    client.subscribe("NorthtoSouth/minoraccident1")
    time.sleep(300)
 elif(number>0.5 and number<=0.75):
    client.subscribe("SouthwesttoNortheast/minoraccident1")
    time.sleep(300)
 else:
    client.subscribe("northeasttosouthwest/minoraccident1")
    time.sleep(300)


client.loop_stop()

client.disconnect()
