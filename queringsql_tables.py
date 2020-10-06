from database_connection import cursor, connection
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
    #closing database connection.
   #     if(connection):
    #        cursor.close()
     #       connection.close()
            print("Data fetched from roadinfo table")


def get_minoraccidents():
  try:
    postgreSQL_select_Query="SELECT * FROM project_minoraccident"
    cursor.execute(postgreSQL_select_Query)
    rows_minoraccident = cursor.fetchall()
    #print(len(rows_minoraccident))
    return(rows_minoraccident)

  finally:
    #closing database connection.
   #     if(connection):
    #        cursor.close()
     #       connection.close()
            print("Data fetched from minoraccident table")





#if __name__ == '__main__':
    #output_getweather=get_weatherdetails()
    #print(output_getweather)
    #output_getroadinfo=get_roadinfo()
    #print(output_getroadinfo)
