import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from joblib import variables as V

# Get job name parameter
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
print(args)

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data from the 'raw_zone' folder
df = spark.read.parquet('s3://ai4e-ap-southeast-1-dev-s3-data-landing/hauct/raw_zone/*')

# Create the 'dim_product' table and export parquet file to 'golden_zone'
df.select("product_id", "category", "sub_category", "product_name") \
    .dropDuplicates() \
    .write.mode('overwrite')\
    .parquet("s3://ai4e-ap-southeast-1-dev-s3-data-landing/hauct/golden_zone/dim_product")

# Create the 'dim_customer' table and export parquet file to 'golden_zone'
df.select("customer_id", "customer_name", "birth_date", "phone_number") \
    .dropDuplicates() \
    .write.mode('overwrite')\
    .parquet("s3://ai4e-ap-southeast-1-dev-s3-data-landing/hauct/golden_zone/dim_customer")

# Create the 'dim_address' table and export parquet file to 'golden_zone'
df.select("address_id", "province", "district", "ward", "ship_cost") \
    .dropDuplicates() \
    .write.mode('overwrite')\
    .parquet("s3://ai4e-ap-southeast-1-dev-s3-data-landing/hauct/golden_zone/dim_address")

# Create the 'dim_order' table and export parquet file to 'golden_zone'
df.select("order_id", "order_date", "ship_date", "customer_id", "address_id", "product_id", "product_number", "revenue", "cost", "discount", "profit") \
    .write.mode('overwrite')\
    .parquet("s3://ai4e-ap-southeast-1-dev-s3-data-landing/hauct/golden_zone/dim_order")