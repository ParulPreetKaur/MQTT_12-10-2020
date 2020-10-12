from database_connection import cursor, connection
import json
import psycopg2
#from publishing_client1 import get_loc1

def get_weatherdetails():
  try:
    postgreSQL_select_Query="SELECT * FROM project_weather"
    #connection.rollback()
    cursor.execute(postgreSQL_select_Query)
    tuple1 = {}
    weatherdetails = cursor.fetchall()
    #print(len(weatherdetails))
    #rows = cursor.fetchall()
    #print(weatherdetails)
    #print("Total rows are:  ", len(rows))
    return(weatherdetails)

    #for row in rows:
     #   print("temperature: ", row[0])
      #  print("chances of rain: ", row[1])
       # print("humidity: ", row[2])
        #print("id: ", row[3])
        #print("\n")
        #tuple1 = row
    #return(tuple1)

    #for row in rows:
     # while row is not None:
      #  tuple1 = row
    #return (tuple1)


  finally:
    #closing database connection.
        #if(connection):
         #   cursor.close()
          #  connection.close()
           print("data fetched successfully")

def get_roadinfo():
  try:
    postgreSQL_select_Query="SELECT * FROM project_roadinfo"
    cursor.execute(postgreSQL_select_Query)
    rows_roadinfo = cursor.fetchall()
    #print("Total rows are:  ", len(rows_roadinfo))
    return(rows_roadinfo)

  finally:
    print("Data fetched from roadinfo table")


def get_minoraccidents():
  try:
    postgreSQL_select_Query="SELECT * FROM project_minoraccident"
    cursor.execute(postgreSQL_select_Query)
    rows_minoraccident = cursor.fetchall()
    #print(len(rows_minoraccident))
    return(rows_minoraccident)

  finally:
    print("Data fetched from minoraccident table")

def sensor_traffic_Handler_from_Client1(Topic, jsonData):
      try:
        json_Dict = json.loads(jsonData)
        traffic = json_Dict['traffic']
        directions = json_Dict['directions']
        select_minortraffic ="select direction from project_minortraffic"
        cursor.execute(select_minortraffic)
        trafficinfo = cursor.fetchall()
        if(trafficinfo[0][0] == directions) :
             postgres_insert_queryupdate="""update project_minortraffic set traffic= %s where direction =%s"""
             cursor.execute(postgres_insert_queryupdate, (traffic,trafficinfo[0][0]))
        elif(trafficinfo[2][0] == directions):
            postgres_insert_queryupdate2 = """update project_minortraffic set traffic= %s where direction =%s"""
            cursor.execute(postgres_insert_queryupdate2, (traffic, trafficinfo[2][0]))
        elif(trafficinfo[3][0] == directions):
            postgres_insert_queryupdate3 = """update project_minortraffic set traffic= %s where direction =%s"""
            cursor.execute(postgres_insert_queryupdate3, (traffic, trafficinfo[3][0]))

        else :
             postgres_insert_queryupdate3 = """update project_minortraffic set traffic= %s where direction =%s"""
             cursor.execute(postgres_insert_queryupdate3, (traffic, trafficinfo[1][0]))
         #postgres_insert_query1 = """ INSERT INTO project_minortraffic (direction, traffic) VALUES (%s,%s)"""
         #record1_to_insert = (directions, traffic)
        #   cursor.execute("Rollback")
         #cursor.execute(postgres_insert_query1, record1_to_insert)
        connection.commit()
        count = cursor.rowcount
        print(count, "Record inserted successfully into minortraffic table")
        return(directions)


      finally:
        print("Inserted successfully")


def sensor_accident_Handler_from_Client1(Topic, jsonData):
  try:
     json_Dict = json.loads(jsonData)
     accident = json_Dict['accident']
     Direction = json_Dict['direction']
     select_minoraccident = "select direction from project_minoraccident"
     cursor.execute(select_minoraccident)
     accidentinfo = cursor.fetchall()
     print(accidentinfo[0])
     if (accidentinfo[0][0] == Direction):
         postgres_queryupdate_accident = """update project_minoraccident set minor_accident= %s where direction =%s"""
         cursor.execute(postgres_queryupdate_accident, (accident, accidentinfo[0][0]))
     elif(accidentinfo[2][0] == Direction):
         postgres_queryupdate2_accident = """update project_minoraccident set minor_accident= %s where direction =%s"""
         cursor.execute(postgres_queryupdate2_accident, (accident, accidentinfo[2][0]))
     elif (accidentinfo[3][0] == Direction):
         print(accidentinfo[3][0])
         postgres_queryupdate3_accident = """update project_minoraccident set minor_accident= %s where direction =%s"""
         cursor.execute(postgres_queryupdate3_accident, (accident, accidentinfo[3][0]))
     else:
         postgres_queryupdate4_accident = """update project_minoraccident set minor_accident= %s where direction =%s"""
         cursor.execute(postgres_queryupdate4_accident, (accident, accidentinfo[1][0]))
     connection.commit()
     count = cursor.rowcount
     print(count, "Record inserted successfully in table")
     return(Direction)

  # traffic = json_Dict['traffic']
  # if (Topic=='client1/accident'):
  # accident = json_Dict['accident']
  # print(accident)
  # print("hello" + jsonData)
  # subscribed_message=['jsonData']
  # print(subscribed_message)
  # for i in range(5, 100):
  # print(i)
  # directions="East to West"
  # directions = get_loc1()
  # msg = [("accident on lane 5 : Switch to lane 3"), ("accident on lane 5 : Switch to lane 10"),("accident on lane 5 : Switch to lane 15")]
  # total_message_client1 = len(msg)
  # total_message_client1 = len(jsonData)
  # for j in range(1, total_message_client1):
  # for subscribed_message in jsonData:
  # directions = "East to West"
  # postgres_insert_query = """ INSERT INTO project_minoraccident (direction, minor_accident) VALUES (%s,%s)"""
  # record_to_insert = (Direction, accident)
  # cursor.execute(postgres_insert_query, record_to_insert)

  finally:
    print("Insert into minor accident table")



    #for record in jsonData:
     #  msg = record
    #print("hello" + str(msg))
    #msg[1]
    #print("parul")
    #postgreSQL_select_Query="SELECT * FROM project_minoraccident"
    #cursor.execute(postgreSQL_select_Query)
    #rows_minoraccident = cursor.fetchall()
    #print(len(rows_minoraccident))
    #return(rows_minoraccident)

  #finally:
     #print("Data inserted from minoraccident table")

#sensor_Data_Handler_from_Client1("client1","traffic at 5")


#if __name__ == '__main__':
    #output_getweather=get_weatherdetails()
    #print(output_getweather)
    #output_getroadinfo=get_roadinfo()
    #print(output_getroadinfo)
