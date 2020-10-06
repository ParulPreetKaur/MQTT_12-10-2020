import time
import paho.mqtt.client as mqtt
#from database_connection import cursor, connection, psycopg2
import psycopg2
#from database_connectionclose import *
# Callback Function on Connection with MQTT Server
def on_connect( client, userdata, flags, rc):
    print("Connected with Code :" +str(rc))
    # Subscribe Topic from here
    client.subscribe("home/#")
    client.subscribe("weather/#")
    client.subscribe("roadinfo/#")
    client.subscribe("directionalroadinfo/minoraccident")
# Callback Function on Receiving the Subscribed Topic/Message
def on_message( client, userdata, msg):
    # print the message received from the subscribed topic
    #print ( str(msg.payload) )
    print("Received message payload: {0}".format(str(msg.payload)))

def get_loc1():
     Lane = 5
     print("Hello from a function")
     if (Lane <= 10):
         print("parul")
         direction = "East to West"
     return (direction)


#output = get_loc()
#print(output)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("192.168.0.13", 1883, 60)

client.loop_start()
time.sleep(1)

#connection.rollback()
while True:
 #def insertfromclient_minoraccidents():

 try:
  connect = psycopg2.connect(user="postgres", password="admin", host="127.0.0.1", port="5432", database="Project")
  cursor = connect.cursor()
  for i in range(5, 100):
         print(i)
         directions = get_loc1()
         msg = [("accident on lane 5 : Switch to lane 3"), ("accident on lane 5 : Switch to lane 10"), ("accident on lane 5 : Switch to lane 15")]
         total_message_client1=len(msg)
         #for j in range(1, total_message_client1):
         for record in msg:
             #j = j+1
             postgres_insert_query = """ INSERT INTO project_minoraccident (user_id, direction, minor_accident) VALUES (%s,%s,%s)"""
             record_to_insert = (i, directions, record)
             cursor.execute("Rollback")
             cursor.execute(postgres_insert_query, record_to_insert)
             i= i+1
             connect.commit()
             count = cursor.rowcount
             print(count, "Record inserted successfully into minoraccident table")

        # cursor.execute("INSERT INTO project_minoraccident (user_id, direction, minor_accident) VALUES(%s, %s, %s)", ('11', directions,msg))
         #"
         #cursor.execute(postgreSQL_select_Query, record_to_insert)
         #connection.commit()
   #except (Exception, psycopg2.Error) as error:
    #       if(connect):
     #          print("Failed to insert record into mobile table", error)

         # finally:
         #   #closing database connection.
         #  if(connection):
         #     cursor.close()
         #    connection.close()
         #   print("PostgreSQL connection is closed")


 except (Exception, psycopg2.Error) as error:
     if (connect):
        print("Error while connecting to PostgreSQL", error)
 finally:
     cursor.close()
     connect.close()
     print("PostgreSQL connect is closed")
     time.sleep(500)
     print("Data inserted into minoraccident table")


#insertfromclient_minoraccidents()
#while True:
    #client.publish("client1", "accident on lane 5 : Switch to lane 3")
    #client.publish("client1", "heavy traffic at Highway417")
    #client.publish("client1", "traffic controller at lane 417:drive acc to speed")
    #print("Message Sent")
    #time.sleep(5)
    #client.publish("Test/temp", "35")
    #client.publish("Testing/temp", "52")
    #latitude="50"
    #client.longitude="60"



    #time.sleep(15)
   # return latitude
client.loop_stop()

client.disconnect()


#if __name__ == '__main__':
 #   output = get_loc1()
  #  print(output)
