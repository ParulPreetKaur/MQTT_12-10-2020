import time
import paho.mqtt.client as mqtt

# Callback Function on Connection with MQTT Server
def on_connect( client, userdata, flags, rc):
    print ("Connected with Code :" +str(rc))
    # Subscribe Topic from here
    client.subscribe("Test/#")
    client.subscribe("weather/#")
    client.subscribe("roadinfo/#")
    client.subscribe("directionalroadinfo/minoraccident")

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
    #client.publish("home/lamp","dim")
    #client.publish("home/television", "switch-off")
    time.sleep(15)

client.loop_stop()
client.disconnect()