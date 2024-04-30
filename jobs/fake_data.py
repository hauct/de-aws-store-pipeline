import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *

from joblib import variables as V

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
print(args)

import time
import calendar
import pandas as pd
import numpy as np
import psycopg2
import random
from datetime import datetime, timedelta

def create_connection(DB_INFO):
    connection = psycopg2.connect(
        host=DB_INFO['DB_HOST'],
        database=DB_INFO['DB_DATABASE'],
        user=DB_INFO['DB_USER'],
        password=DB_INFO['DB_PASSWORD']
    )
    connection.set_session(autocommit=True)
    return connection

# Function to create a random unique string
def generate_unique_string():
    timestamp = int(time.time() * 1000)
    random_number = random.randint(10000, 99999)
    unique_string = f"{timestamp}_{random_number}"
    return unique_string

# Function generate a random name
def generate_name():
    names = [
        "John", "Jane", "Mary", "James", "Emily", "Michael", "Sarah", "Jessica", "Jacob", "Mohammed",
        "Sophia", "Ethan", "Madison", "Emma", "Mia", "Oliver", "Noah", "Ava", "Isabella", "Liam",
        "Mason", "Lucas", "Elijah", "Aiden", "Benjamin", "Abigail", "Logan", "Alex", "Nathan", "Grace"
    ]
    name = random.choice(names)
    return name

# Function generate a random birthday
def generate_birthdate():
    birth_year = datetime.now().year - random.randint(18, 70)
    birth_month = random.randint(1, 12)
    _, num_days = calendar.monthrange(birth_year, birth_month)
    birth_day = random.randint(1, num_days)
    birthdate = f"{birth_year}-{birth_month:02d}-{birth_day:02d}"
    return birthdate

# Function to fetch one random record from a specified table in the database
def fetch_one_random_record(DB_INFO, schema, table_name, columns):
    connection = create_connection(DB_INFO)
    cur = connection.cursor()
    cur.execute(f"SELECT {', '.join(columns)} FROM {schema}.{table_name} ORDER BY RANDOM() LIMIT 1;")
    row = cur.fetchone()
    connection.close()
    record_dict = pd.DataFrame([row], columns=columns).to_dict(orient='records')[0]
    return record_dict

# Function to generate customer information
def generate_customer_info():
    customer_id = str(generate_unique_string())
    customer_name = str(generate_name())
    birth_date = str(generate_birthdate())
    phone_number = '09' + str(random.randint(10000000, 99999999))
    address_info = fetch_one_random_record(DB_INFO, 'hauct_endcourse_data', 'address',  ['address_id', 'province', 'district', 'ward', 'price'])
    return {'customer_id':customer_id, 
            'customer_name':customer_name,
            'birth_date':birth_date,
            'phone_number':phone_number,
            'address_id':address_info['address_id'],
            'province':address_info['province'],
            'district':address_info['district'],
            'ward':address_info['ward'],
            'ship_cost':int(address_info['price'])
    }

# Function to generate product information
def generate_product_info():
    product_info = fetch_one_random_record(DB_INFO, 'hauct_endcourse_data', 'product',  ['product_id', 'category', 'sub_category', 'product_name', 'buying_price', 'selling_price'])
    return {'product_id':product_info['product_id'],
            'category':product_info['category'],
            'sub_category':product_info['sub_category'],
            'product_name':product_info['product_name'],
            'buying_price':float(product_info['buying_price']),
            'selling_price':float(product_info['selling_price'])
    }
    
# Function to generate a timestamp for a given date
def generate_date_time(report_date):
    report_date1 = datetime.strptime(report_date, '%Y-%m-%d')

    year = report_date1.year
    month = report_date1.month
    day = report_date1.day

    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    timestamp = datetime(year, month, day, hour, minute, second).strftime('%Y-%m-%d %H:%M:%S')

    return timestamp

# Function to generate a log for a given day
def generate_log(date):
    # Access the global variable customer_list
    global customer_list

    # Generate customer information
    # If customer_list is not empty and a random choice between True and False is True,
    # choose a random customer from customer_list
    # Otherwise, generate new customer information and add it to customer_list if it's not already there
    if customer_list and random.choice([True, False]):
        customer_info = random.choice(customer_list)
    else:
        customer_info = generate_customer_info()
        if customer_info not in customer_list:
            customer_list.append(customer_info)

    # Generate product information
    product_info = generate_product_info()

    # Generate order information
    order_id = str(generate_unique_string())
    order_date = generate_date_time(date)
    ship_date = str(datetime.strptime(order_date, '%Y-%m-%d %H:%M:%S') + timedelta(days=random.randint(1, 3)))

    # Generate a random number of products for the order
    product_number = random.randint(1, 20)

    # Calculate the discount value, revenue, cost, and profit for the order
    discount = float(np.random.choice([0, 0.05, 0.1, 0.15], p=np.array([65, 20, 10, 5])/100))
    revenue = product_number*product_info['selling_price']
    cost = product_number*product_info['buying_price'] + customer_info['ship_cost'] + discount*product_number*product_info['buying_price']
    profit = revenue - cost

    # Create a record of the order
    record = {
        'order_id':str(order_id),
        'order_date':str(order_date),
        'ship_date':str(ship_date),   
        'customer_id':str(customer_info['customer_id']),
        'customer_name':str(customer_info['customer_name']),
        'birth_date':str(customer_info['birth_date']),
        'phone_number':str(customer_info['phone_number']),
        'address_id':str(customer_info['address_id']),
        'province':str(customer_info['province']),
        'district':str(customer_info['district']),
        'ward':str(customer_info['ward']),
        'ship_cost':str(customer_info['ship_cost']),
        'product_id':str(product_info['product_id']),
        'category':str(product_info['category']),
        'sub_category':str(product_info['sub_category']),
        'product_name':str(product_info['product_name']),
        'buying_price':str(product_info['buying_price']),
        'selling_price':str(product_info['selling_price']),
        'product_number':str(product_number),
        'revenue':str(revenue),
        'cost':str(cost),
        'discount': str(discount),
        'profit': str(profit)
    }
    return record

def list_dates(start, end):
    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")
    delta = timedelta(days=1)
    current_date = start_date
    dates = []
    while current_date <= end_date:
        dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += delta
    return dates

def export_to_parquet(date_list):
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    for date in date_list:
        data = []
        for _ in range(random.randint(1,30)):
            record = generate_log(date)
            data.append(record)
            
        # Convert list to Spark DataFrame
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("ship_date", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("birth_date", StringType(), True),
            StructField("phone_number", StringType(), True),
            StructField("address_id", StringType(), True),
            StructField("province", StringType(), True),
            StructField("district", StringType(), True),
            StructField("ward", StringType(), True),
            StructField("ship_cost", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("sub_category", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("buying_price", StringType(), True),
            StructField("selling_price", StringType(), True),
            StructField("product_number", StringType(), True),
            StructField("revenue", StringType(), True),
            StructField("cost", StringType(), True),
            StructField("discount", StringType(), True),
            StructField("profit", StringType(), True)
            ])

        df = spark.createDataFrame(data, schema)\
            .withColumn("order_date", to_timestamp(col("order_date"), 'yyyy-MM-dd HH:mm:ss'))\
            .withColumn("ship_date", to_timestamp(col("ship_date"), 'yyyy-MM-dd HH:mm:ss'))\
            .withColumn("birth_date", to_timestamp(col("birth_date"), 'yyyy-MM-dd HH:mm:ss'))

        # Write DataFrame to Parquet file on S3
        df.write.mode('overwrite').parquet(f's3://ai4e-ap-southeast-1-dev-s3-data-landing/hauct/raw_zone/log_{date}.parquet')

        print(f'Export log {date} successfully')

DB_INFO = {
    'DB_HOST':'introduction-01-intro-ap-southeast-1-dev-introduction-db.cpfm8ml2cxp2.ap-southeast-1.rds.amazonaws.com',
    'DB_DATABASE':'postgres',
    'DB_USER':'postgres',
    'DB_PASSWORD':'postgres123',
    'DB_SCHEMA':'hauct_endcourse_data'
}

customer_list = []

date_list = list_dates('2024-04-01', '2024-04-30')        
export_to_parquet(date_list)