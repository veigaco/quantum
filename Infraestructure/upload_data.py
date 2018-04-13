import pandas as pd
#import MySQLdb as mdb
from datetime import date, datetime, timedelta
#import mysql.connector
import pymysql
import pymysql.cursors

# Connect to the database
connection = pymysql.connect(host='localhost',
                             user='quantum_user',
                             password='Qu4ntum_u$3r',
                             db='securities_master_database',
                             charset='utf8',
                             cursorclass=pymysql.cursors.DictCursor)




## upload basic structure
path ="/Users/Yes/Quantum/Infraestructure/"
industries=pd.read_csv(path+"industries.csv")
instrument=pd.read_csv(path+"instrument.csv")
instrument_type=pd.read_csv(path+"instrument_type.csv")
sectors=pd.read_csv(path+"sectors.csv")

## add industries:
add_industries = ("INSERT INTO industries "
               "(id, name) "
               "VALUES (%s, %s)")
add_instrument = ("INSERT INTO instrument "
  "(id, instrument_type_id, industry_id, sector_id, database_name, type_database, name, description) "
  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
add_instrument_type = ("INSERT INTO instrument_type "
               "(id, name) "
               "VALUES (%s, %s)")
add_sectors = ("INSERT INTO sectors "
               "(id, name) "
               "VALUES (%s, %s)")

## add data
data_industries = [tuple([int(x[0]),x[1]])for x in industries.values]
with connection.cursor() as cursor: # Create a new record
  for j in data_industries:
    cursor.execute(add_industries,j)


data_instrument_type = [tuple([int(x[0]),x[1]])for x in instrument_type.values]
with connection.cursor() as cursor: # Create a new record
  for j in data_instrument_type:
    cursor.execute(add_instrument_type, j)

data_sectors = [tuple([int(x[0]),x[1]])for x in sectors.values]
with connection.cursor() as cursor: # Create a new record
  for j in data_sectors:
    cursor.execute(add_sectors, j)



data_instrument=[tuple([int(x[0]),int(x[1]),int(x[2]),int(x[3]),str(x[4]),str(x[5]),str(x[6]),str(x[7])])for x in instrument.values]
with connection.cursor() as cursor: # Create a new record
  for i in data_instrument:
    cursor.execute(add_instrument,i)


connection.commit()
connection.close()


