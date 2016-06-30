### find the latitude and longtitude for a given airport

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2


def connect_to_db():
    
    host   = 'postgresinstance.c0dzm7nvb0nh.us-west-2.rds.amazonaws.com:5432'
    dbname = 'flysmart'
    
    with open('/home/ubuntu/db_credentials', 'r') as f:
        credentials = f.readlines()
        f.close()
        
    user = credentials[0].rstrip()
    pswd = credentials[1].rstrip()
    
    connection = psycopg2.connect(
        database=dbname,
        user=user,
        password=pswd,
        host=host.split(':')[0],
        port=5432)
    
    return connection


# connect:
con = connect_to_db()


# query: 
def airport_lat_long(Airport):
    sql_query = """
        SELECT latitude, longitude FROM airports
        WHERE faa =  '""" + Airport + "';"
   
    location = pd.read_sql_query(sql_query, con)
    latitude = location.ix[0, 0]
    longitude = location.ix[0, 1]
    return [latitude, longitude]

