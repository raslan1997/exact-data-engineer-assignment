import csv
import pandas as pd 
import pyarrow
from pyspark.sql import SparkSession
from sqlalchemy import create_engine
from endpoints import Controller
import psycopg2
from psycopg2 import Error
import sys
from pyspark.sql import Row
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import databricks.koalas as ks

#defining parameters
param_dic = {
    "host"      : "localhost",
    "database"  : "exact_yellow_cap",
    "user"      : "postgres",
    "password"  : "3690"
}


sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

df2 = spark.read.parquet("./tmp/ny_yc_trip_data_cleansed.parquet")

df = pd.read_parquet("./tmp/ny_yc_trip_data_cleansed.parquet")

counter = df2.count()

print(df.head(10))
print(counter)


#connecting to the database

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    return conn


# Connecting to the database
#conn = connect(param_dic)

def single_insert(conn, insert_req):
    """ Execute a single INSERT request """
    cursor = conn.cursor()
    try:
        cursor.execute(insert_req)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()

#16318975

con = psycopg2.connect(database="exact_yellow_cap", user="postgres", password="3690", host="127.0.0.1", port="5432")
print("Database opened successfully")

cur = con.cursor()
for i in range(0,counter):

    mySql_insert_query =   """INSERT INTO ny_yc_trip ("vendorid","tpep_pickup_datetime" ,"tpep_dropoff_datetime", "passenger_count","trip_distance" ,"ratecodeid","store_and_fwd_flag","pulocationid" ,"dolocationid","payment_type","fare_amount" ,"extra","mta_tax","tip_amount","tolls_amount","improvement_surcharge","total_amount","congestion_surcharge") 
    VALUES (%s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s);"""
    recordTuple = (df['VendorID'][i],df['tpep_pickup_datetime'][i],df['tpep_dropoff_datetime'][i],int(df['passenger_count'][i]),int(df['trip_distance'][i]),df['RatecodeID'][i],df['store_and_fwd_flag'][i],int(df['PULocationID'][i]),int(df['DOLocationID'][i]),int(df['payment_type'][i]),df['fare_amount'][i],df['extra'][i],df['mta_tax'][i],df['tip_amount'][i],df['tolls_amount'][i],df['improvement_surcharge'][i],df['total_amount'][i],df['congestion_surcharge'][i])
    cur.execute(mySql_insert_query, recordTuple)
con.commit()
print("Record inserted successfully")
con.close()


#,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s


#"vendorid","tpep_pickup_datetime" ,"tpep_dropoff_datetime",
#"passenger_count","trip_distance" ,"ratecodeid"
#,"store_and_fwd_flag","pulocationid" ,"dolocationid",
#"payment_type","fare_amount" ,"extra",
#"mta_tax","tip_amount","tolls_amount",
#"improvement_surcharge","total_amount","congestion_surcharge"


#df['VendorID'][i],df['tpep_pickup_datetime'][i],df['tpep_dropoff_datetime'][i],
#int(df['passenger_count'][i]),int(df['trip_distance'][i]),df['RatecodeID'][i]
#,df['store_and_fwd_flag'][i],int(df['PULocationID'][i]),int(df['DOLocationID'][i])
#,int(df['payment_type'][i]),df['fare_amount'][i],df['extra'][i],
#df['mta_tax'][i],df['tip_amount'][i],df['tolls_amount'][i],
#df['improvement_surcharge'][i],df['total_amount'][i],df['congestion_surcharge'][i])
#