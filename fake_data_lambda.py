# Importing necessary libraries
import time
import calendar
import pandas as pd
import numpy as np
import psycopg2
import random
from datetime import datetime, timedelta

# Importing database credentials and utility functions
from credentials.db_info import DB_INFO
from credentials.db_utils import fetch_data_by_sql

# Setting up logging
import logging
import traceback

# Setting up AWS services
import boto3
from io import BytesIO

customer_list = []

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

# Function to create a connection to the database
def create_connection(DB_INFO):
    connection = psycopg2.connect(
        host=DB_INFO['DB_HOST'],
        database=DB_INFO['DB_DATABASE'],
        user=DB_INFO['DB_USER'],
        password=DB_INFO['DB_PASSWORD']
    )
    connection.set_session(autocommit=True)
    return connection

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
    profit = round(revenue - cost,2)

    # Create a record of the order
    record = {
        'order_id':order_id,
        'order_date':order_date,
        'ship_date':ship_date,   
        'customer_id':customer_info['customer_id'],
        'customer_name':customer_info['customer_name'],
        'birth_date':customer_info['birth_date'],
        'phone_number':customer_info['phone_number'],
        'address_id':customer_info['address_id'],
        'province':customer_info['province'],
        'district':customer_info['district'],
        'ward':customer_info['ward'],
        'ship_cost':customer_info['ship_cost'],
        'product_id':product_info['product_id'],
        'category':product_info['category'],
        'sub_category':product_info['sub_category'],
        'product_name':product_info['product_name'],
        'buying_price':product_info['buying_price'],
        'selling_price':product_info['selling_price'],
        'product_number': product_number,
        'revenue':revenue,
        'cost': cost,
        'discount': discount,
        'profit': profit
    }
    return record

def fake_data_lh(event, context):
    try:
        # Setting up the logger
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        # Initialize an empty list to store the data
        data = []

        date_list = event['data_list']
        bucket = event['bucket']
        prefix = event['prefix']

        s3 = boto3.client('s3')

        # For each date in date_list, generate a random number of logs and append them to data
        for date in date_list:
            for _ in range(random.randint(1,15)):
                record = generate_log(date)
                data.append(record)

            # Convert data to a pandas DataFrame and export it to a parquet file
            df = pd.DataFrame(data)
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, engine='pyarrow', compression='snappy')

            # Upload the parquet file to S3
            s3.put_object(Bucket=bucket, Key=f'{prefix}log_{date}.parquet', Body=parquet_buffer.getvalue())
            
            # Print a success message
            print(f'Export log {date} to S3 successfully')
        return event
    except Exception as ex:
        logger.error(f'FATAL ERROR: {ex} %s')
        logger.error('TRACEBACK:')
        logger.error(traceback.format_exc())
        return {"status": "FAIL", "error": f"{ex}"}