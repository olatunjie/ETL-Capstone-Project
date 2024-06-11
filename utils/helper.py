import requests
import json
import psycopg2
import configparser
import logging
import boto3
from datetime import datetime
import pandas as pd
import redshift_connector
from botocore.exceptions import NoCredentialsError
from sqlalchemy import create_engine

from utils.categories import data_categories
from utils.categories import data_categories_1
from utils.categories import data_categories_2
from sql_statements.create import dev_tables,transformed_tables,local_tables
from sql_statements.transform import transformation_queries

# Get today's date
today = datetime.today()
formatted_date = today.strftime("%d%m%Y")

config = configparser.ConfigParser()

# Read from .env file
config.read('.env')

dbname = config['LOCALDB']['dbname']
user = config['LOCALDB']['user']
password = config['LOCALDB']['password']
host = config['LOCALDB']['host']
port = config['LOCALDB']['port']

api_url = config['API']['url']
api_key = config['API']['X-RapidAPI-Key']
api_host = config['API']['X-RapidAPI-Host']

access_key = config['AWS']['access_key']
secret_key = config['AWS']['secret_key']
bucket_name = config['AWS']['bucket_name']
region_name = config['AWS']['region_name']
role = config['AWS']['arn']

dwh_host = config['DWH_CONN']['host']
dwh_user = config['DWH_CONN']['user']
dwh_password = config['DWH_CONN']['password']
dwh_database = config['DWH_CONN']['database']
dwh_port = config['DWH_CONN']['port']
#timeouttime = config['DWH_CONN']['timeout']


dev_schema = config['MISC']['dev_schema']
prod_schema = config['MISC']['prod_schema']
staging_schema = config['MISC']['staging_schema']

headers = {
	"X-RapidAPI-Key": api_key,
	"X-RapidAPI-Host": api_host,
 'Content-Type': 'application/json;charset=UTF-8'
}

def api_helper(url,headers):
    response = requests.get(url, headers=headers)
    return response


# Connect to the PostgreSQL database
conn = psycopg2.connect(
dbname= dbname,
user= user,
password= password,
host= host,
port= port)  

# Connects to Redshift cluster using AWS credentials
dwh_conn = psycopg2.connect(
    host=dwh_host,
    database=dwh_database,
    user=dwh_user,
    password=dwh_password,
    port=dwh_port
 )

# Connects to Redshift cluster using AWS credentials
redshift_conn = redshift_connector.connect(
    host=dwh_host,
    database=dwh_database,
    user=dwh_user,
    password=dwh_password,
    port=dwh_port
 )



# Define a function to insert data into the database
def insert_data(json_data,table_name):
    cursor = conn.cursor()
    # Parse the JSON data
    data = json.loads(json_data)
    for symbol, values in data.items():
        name = values['name']
        units = values['Units'].replace('--','N/A')
        price = float(values['Price'].replace(',', '').replace('--','0'))  # Remove comma from price and convert to float
        change = float(values['Change'].replace(',', '').replace('--','0').replace('+',''))
        percent_change = float(values['%Change'].replace(',', '').replace('--','0').replace('+','').replace('%',''))
        contract = values['Contract']
        time_edt = values['Time (EDT)'].replace('--','')
        cursor.execute(f"INSERT INTO {table_name} (symbol, name, units, price, change, percent_change, contract, time_edt) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", 
                       (symbol, name, units, price, change, percent_change, contract, time_edt))
        conn.commit()
    
# Define a function to insert commodity data into the database
def insert_commodity_data(json_data,table_name):
    cursor = conn.cursor()
    # Parse the JSON data
    data = json.loads(json_data)
    for symbol, values in data.items():
        name = values['name']
        value = float(values['Value'].replace(',', '').replace('--','0').replace('USd/lb.','0').replace('USD/t oz.','0').replace('USd/bu.','0').replace('USD/MT','0').replace('USd/gal.','0').replace('USD/bbl.','0').replace('USD/MMBtu','0'))  # Remove comma from price and convert to float
        change = float(values['Change'].replace(',', '').replace('--','0').replace('+',''))
        percent_change = float(values['%Change'].replace(',', '').replace('--','0').replace('+','').replace('%',''))
        high = float(values['High'].replace(',', '').replace('--','0').replace('+','').replace('%',''))
        low = float(values['Low'].replace(',', '').replace('--','0').replace('+','').replace('%','').replace('Jul 2024', '0').replace('Jun 2024', '0').replace('N/A', '0'))
        time_edt = values['Time (EDT)']
        cursor.execute(f"INSERT INTO {table_name} (symbol, name, value, change, percent_change, high,low, time_edt) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", 
                       (symbol, name, value, change, percent_change, high,low, time_edt))
        conn.commit()

#Extract Data and Save in json file    
def extract_api_data():
    for category in data_categories:
        response = api_helper(api_url + '' + category ,headers)
        data = response.json()
    
    # Define the file path
    file_path = f'./api_data/{category}_{formatted_date}.json'
    
    # Write JSON data to file
    with open(file_path, "w") as json_file:
       json.dump(data, json_file, indent=4)
        
def read_commodity_data():
    for category in data_categories_1:
        with open(f'./api_data/{category}_{formatted_date}.json') as user_file:
            file_contents = user_file.read()
  
            insert_commodity_data(file_contents,category)  
        
def read_other_data():
    for category in data_categories_2:
        with open(f'./api_data/{category}_{formatted_date}.json') as user_file:
            file_contents = user_file.read()
  
            insert_data(file_contents,category)  

# Create s3 Bucket (Data Lake)
def create_bucket():
    client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    s3 = boto3.resource(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
        )
    
    status = s3.Bucket(bucket_name) in s3.buckets.all()  
      
    if status is False:
        client.create_bucket(
        Bucket= bucket_name,
        CreateBucketConfiguration={
            'LocationConstraint': region_name,
        },
    )

def Save_data_to_bucket():
    for table in data_categories:
        query = f'SELECT * FROM {table}'
        df = pd.read_sql_query(query,conn)

        df.to_csv(
            f's3://{bucket_name}/{table}.csv',index=False,storage_options={'key' : access_key,'secret': secret_key}
            ) 
        
def create_local_dev_tables():
    cursor = conn.cursor()
        # Create the tables
    for query in local_tables:
        cursor.execute(query)
        conn.commit()
    
def create_dwh_dev():
    # Create the Schema
    # print('---STEP 1---')
    cursor = dwh_conn.cursor()
    cursor.execute(f'CREATE SCHEMA {dev_schema}')
    # print('---STEP 2---')
    dwh_conn.commit()
    # Create the tables
    for query in dev_tables:
        cursor.execute(query)
        dwh_conn.commit()

def create_dwh_star():
    # Create the Schema
    # print('---STEP 1---')
    cursor = dwh_conn.cursor()
    cursor.execute(f'CREATE SCHEMA {prod_schema}')
    # print('---STEP 2---')
    dwh_conn.commit()
    # Create the tables
    for query in transformed_tables:
        cursor.execute(query)
        dwh_conn.commit()

def copy_data_to_redshift():
    for table in data_categories:
        transfer_data(dev_schema,table)



def transfer_data(schema_name,table_name):

    _n = f'{schema_name}.{table_name}'

    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
        options="-c standard_conforming_strings=off"
    )
    pg_cursor = pg_conn.cursor()

    # Fetch data from PostgreSQL table
    pg_cursor.execute(f"SELECT * FROM public.{table_name}")
    data = pg_cursor.fetchall()

    df_data = pd.read_sql_query(f"SELECT * FROM public.{table_name}",pg_conn)
    print(_n)
    print(df_data.head(1))
    print('----STEP 1----')
    with redshift_conn.cursor() as _cursor:
         _cursor.write_dataframe(df_data,_n)
    print('----STEP 2----')
    # Close PostgreSQL connection
    pg_cursor.close()
    pg_conn.close()

    # Commit and close Redshift connection
    redshift_conn.commit()
    _cursor.close()
#redshift_conn.close()


def copy_data_to_dwh():
    cursor = dwh_conn.cursor()
    for query in transformation_queries:
        cursor.execute(query)
        dwh_conn.commit()
