#from database_connection import cursor, connection
#cursor.execute('''
                    #INSERT INTO project_minoraccident (user_id, direction, minor_accident)
                    #VALUES
                    #('6','East to West','accident on lane 5 : Switch to lane 3'),
                    #('7','East to West','heavy traffic at Highway417')
                    #''')
#connection.commit()
#direction = "East"
#msg= "parul"
#cursor.execute("INSERT INTO project_minoraccident (user_id, direction, minor_accident) VALUES(%s, %s, %s)", ('10', direction,msg))
#postgreSQL_select_Query ="INSERT INTO project_minoraccident(user_id, direction, minor_accident) VALUES( % s, % s, % s)"
#record = ('5', 'One Plus 6', 950)
#cursor.execute(postgreSQL_select_Query, record)
#connection.commit()
#def get_loc1():
 # Lane = 15


#if(Lane <= 10):
 # print(Lane)


#import psycopg2

#try:
 #  connection = psycopg2.connect(user="postgres",
                                  #password="admin",
                                  #host="127.0.0.1",
                                  #port="5432",
                                  #database="Project")
  # cursor = connection.cursor()

#postgres_insert_query = """ INSERT INTO project_minoraccident (user_id, direction, minor_accident) VALUES (%s,%s,%s)"""
#record_to_insert = (6, 'One Plus 6', 950)
#cursor.execute(postgres_insert_query, record_to_insert)

#connection.commit()
#count = cursor.rowcount
#print (count, "Record inserted successfully into mobile table")

#except (Exception, psycopg2.Error) as error :
 #   if(connection):
  #      print("Failed to insert record into mobile table", error)

#finally:
 #   #closing database connection.
  #  if(connection):
   #     cursor.close()
    #    connection.close()
     #   print("PostgreSQL connection is closed")
