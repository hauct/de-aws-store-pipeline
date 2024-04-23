import pandas as pd
from sqlalchemy import create_engine
from credentials.db_info import DB_INFO

# Setting up logging
import logging
import traceback

# Setting up AWS services
import boto3
from io import BytesIO

# Function to get a file list in S3
def get_s3_file_list(bucket, prefix):
    s3 = boto3.client('s3')
    files = []
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in response['Contents']:
        files.append(obj['Key'])
    return files

# Function to read a parquet file and return a DataFrame
def read_parquet_to_df(file_path):
    df = pd.read_parquet(file_path)
    return df

# Function to insert data into the 'dim_product' table
def insert_data_to_product_table(df, DB_INFO):
    # Select necessary columns from the DataFrame
    product_df = df[['product_id', 'category', 'sub_category', 'product_name', 'buying_price', 'selling_price']].drop_duplicates(subset=['product_id'])

    # Create a connection to the database
    engine = create_engine(f"postgresql://{DB_INFO['DB_USER']}:{DB_INFO['DB_PASSWORD']}@{DB_INFO['DB_HOST']}/{DB_INFO['DB_DATABASE']}")

    # Create an SQL statement to create the 'dim_product' table with necessary columns and data types
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {DB_INFO['DB_SCHEMA']}.dim_product (
        product_id VARCHAR PRIMARY KEY,
        category VARCHAR,
        sub_category VARCHAR,
        product_name VARCHAR
    )
    """
    # Execute the create table query
    with engine.begin() as transaction:
        transaction.execute(create_table_query)

    # Insert data from the DataFrame into the 'dim_product' table, with 'product_id' as the primary key
    for index, row in product_df.iterrows():
        insert_product_query = f"""
        INSERT INTO {DB_INFO['DB_SCHEMA']}.dim_product (product_id, category, sub_category, product_name)
        VALUES ('{row['product_id']}', '{row['category']}', '{row['sub_category']}', '{row['product_name']}')
        ON CONFLICT (product_id) DO NOTHING
        """
        # Execute the insert query
        with engine.begin() as transaction:
            transaction.execute(insert_product_query)
            
# Function to insert data into the 'dim_customer' table
def insert_data_to_customer_table(df, DB_INFO):
    # Select necessary columns from the DataFrame
    customer_df = df[['customer_id', 'customer_name', 'birth_date', 'phone_number']].drop_duplicates(subset=['customer_id'])

    # Create a connection to the database
    engine = create_engine(f"postgresql://{DB_INFO['DB_USER']}:{DB_INFO['DB_PASSWORD']}@{DB_INFO['DB_HOST']}/{DB_INFO['DB_DATABASE']}")

    # Create an SQL statement to create the 'dim_customer' table with necessary columns and data types
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {DB_INFO['DB_SCHEMA']}.dim_customer (
        customer_id VARCHAR PRIMARY KEY,
        customer_name VARCHAR,
        birth_date DATE,
        phone_number INTEGER
    )
    """
    # Execute the create table query
    with engine.begin() as transaction:
        transaction.execute(create_table_query)

    # Insert data from the DataFrame into the 'dim_customer' table, with 'customer_id' as the primary key
    for index, row in customer_df.iterrows():
        insert_product_query = f"""
        INSERT INTO {DB_INFO['DB_SCHEMA']}.dim_customer (customer_id, customer_name, birth_date, phone_number)
        VALUES ('{row['customer_id']}', '{row['customer_name']}', '{row['birth_date']}', '{row['phone_number']}')
        ON CONFLICT (customer_id) DO NOTHING
        """
        # Execute the insert query
        with engine.begin() as transaction:
            transaction.execute(insert_product_query)

def insert_data_to_address_table(df, DB_INFO):
    # Select necessary columns from the DataFrame
    address_df = df[['address_id', 'province', 'district', 'ward', 'ship_cost']].drop_duplicates(subset=['address_id'])

    # Create a connection to the database
    engine = create_engine(f"postgresql://{DB_INFO['DB_USER']}:{DB_INFO['DB_PASSWORD']}@{DB_INFO['DB_HOST']}/{DB_INFO['DB_DATABASE']}")

    # Create an SQL statement to create the 'dim_address' table with necessary columns and data types
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {DB_INFO['DB_SCHEMA']}.dim_address (
        address_id VARCHAR PRIMARY KEY,
        province VARCHAR,
        district VARCHAR,
        ward VARCHAR,
        ship_cost INTEGER
    )
    """
    # Execute the create table query
    with engine.begin() as transaction:
        transaction.execute(create_table_query)

    # Insert data from the DataFrame into the 'dim_address' table, with 'address_id' as the primary key
    for index, row in address_df.iterrows():
        insert_product_query = f"""
        INSERT INTO {DB_INFO['DB_SCHEMA']}.dim_address (address_id, province, district, ward, ship_cost)
        VALUES ('{row['address_id']}', '{row['province']}', '{row['district']}', '{row['ward']}', '{row['ship_cost']}')
        ON CONFLICT (address_id) DO NOTHING
        """
        # Execute the insert query
        with engine.begin() as transaction:
            transaction.execute(insert_product_query)
            
def insert_data_to_order_table(df, DB_INFO):
    # Select necessary columns from the DataFrame
    order_df = df[['order_id', 'order_date', 'ship_date', 'customer_id', 'address_id', 'product_id', 'product_number', 'revenue', 'cost', 'discount', 'profit']].drop_duplicates(subset=['order_id'])

    # Create a connection to the database
    engine = create_engine(f"postgresql://{DB_INFO['DB_USER']}:{DB_INFO['DB_PASSWORD']}@{DB_INFO['DB_HOST']}/{DB_INFO['DB_DATABASE']}")

    # Create an SQL statement to create the 'dim_order' table with necessary columns and data types
    create_table_query = f"""
CREATE TABLE IF NOT EXISTS {DB_INFO['DB_SCHEMA']}.dim_order (
    order_id VARCHAR PRIMARY KEY,
    order_date DATE,
    ship_date DATE,
    customer_id VARCHAR,
    address_id VARCHAR,
    product_id VARCHAR,
    product_number INTEGER,
    revenue FLOAT,
    cost FLOAT,
    discount FLOAT,
    profit FLOAT,
    FOREIGN KEY (customer_id) REFERENCES {DB_INFO['DB_SCHEMA']}.dim_customer(customer_id),
    FOREIGN KEY (address_id) REFERENCES {DB_INFO['DB_SCHEMA']}.dim_address(address_id),
    FOREIGN KEY (product_id) REFERENCES {DB_INFO['DB_SCHEMA']}.dim_product(product_id)
)
    """
    # Execute the create table query
    with engine.begin() as transaction:
        transaction.execute(create_table_query)

    # Insert data from the DataFrame into the 'dim_order' table, with 'order_id' as the primary key
    for index, row in order_df.iterrows():
        insert_product_query = f"""
        INSERT INTO {DB_INFO['DB_SCHEMA']}.dim_order (order_id, order_date, ship_date, customer_id, address_id, product_id, product_number, revenue, cost, discount, profit)
        VALUES ('{row['order_id']}', '{row['order_date']}', '{row['ship_date']}', '{row['customer_id']}', '{row['address_id']}', '{row['product_id']}', '{row['product_number']}', '{row['revenue']}', '{row['cost']}', '{row['discount']}', '{row['profit']}')
        ON CONFLICT (order_id) DO NOTHING
        """
        # Execute the insert query
        with engine.begin() as transaction:
            transaction.execute(insert_product_query)
            
def load_data_lh(event, context):    
    try:
        # Setting up the logger
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        
        bucket = event['bucket']
        prefix = event['prefix']

        file_path_list = get_s3_file_list(bucket, prefix)
        
        for file_path in file_path_list:            
            # Read the parquet file and convert it to a DataFrame
            df = read_parquet_to_df(file_path)
            # Insert data from the DataFrame into the 'dim_product' table
            insert_data_to_product_table(df, DB_INFO)
            # Insert data from the DataFrame into the 'dim_customer' table
            insert_data_to_customer_table(df, DB_INFO)
            # Insert data from the DataFrame into the 'dim_address' table
            insert_data_to_address_table(df, DB_INFO)
            # Insert data from the DataFrame into the 'dim_order' table
            insert_data_to_order_table(df, DB_INFO)
            
            print(f'Ingested data from {file_path.split("/")[-1]} to DB successfully')
    except Exception as ex:
        logger.error(f'FATAL ERROR: {ex} %s')
        logger.error('TRACEBACK:')
        logger.error(traceback.format_exc())
        return {"status": "FAIL", "error": f"{ex}"}
            