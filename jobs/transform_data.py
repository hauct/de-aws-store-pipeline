import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *

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

# Read data from 'golden_zone'
dim_product_df = spark.read.parquet('s3://ai4e-ap-southeast-1-dev-s3-data-landing/hauct/golden_zone/dim_product/*')
dim_customer_df = spark.read.parquet('s3://ai4e-ap-southeast-1-dev-s3-data-landing/hauct/golden_zone/dim_customer/*')
dim_address_df = spark.read.parquet('s3://ai4e-ap-southeast-1-dev-s3-data-landing/hauct/golden_zone/dim_address/*')
dim_order_df = spark.read.parquet('s3://ai4e-ap-southeast-1-dev-s3-data-landing/hauct/golden_zone/dim_order/*')

# Create the 'overview' table
overview = dim_order_df\
.withColumn('report_date', to_date('order_date'))\
.groupBy('report_date')\
.agg(count('order_id').alias('num_order_id'), countDistinct('customer_id').alias('num_customer_id')\
      , sum('revenue').alias('total_revenue'), sum('cost').alias('total_cost')\
      , sum('profit').alias('total_profit') )\
.orderBy('report_date')

# Create the 'profit_customer' table
profit_customer = dim_order_df\
.withColumn('report_date', to_date('order_date'))\
.groupBy('report_date', 'customer_id')\
.agg(count('order_id').alias('num_order_id'), sum('revenue').alias('total_revenue')\
     , sum('cost').alias('total_cost'), sum('profit').alias('total_profit'))\
.orderBy(col('report_date').desc(), col('total_profit').desc())

# Create the 'profit_distribute' table
profit_distribute = dim_order_df\
.withColumn('report_date', to_date('order_date'))\
.join(dim_address_df, 'address_id', 'inner')\
.join(dim_product_df, 'product_id', 'inner')\
.join(dim_customer_df, 'customer_id', 'inner')\
.withColumn("age", (datediff(current_date(), col("birth_date")) / 365).cast("integer"))\
.withColumn('age_type', when((col('age')>=18)&(col('age')<25), '18-24')\
                       .when((col('age')>=25)&(col('age')<35), '25-34')\
                       .when((col('age')>=35)&(col('age')<45), '35-44')\
                       .when((col('age')>=45)&(col('age')<55), '45-54')\
                       .when((col('age')>=55)&(col('age')<65), '55-64')\
                       .when(col('age')>=65, '65 and over')\
                       .when(col('age')<18, 'under 18'))\
.groupBy('report_date', 'category', 'sub_category', 'district', 'age_type')\
.agg(sum('profit').alias('total_profit'))\
.orderBy(col('report_date').desc(), col('total_profit').desc())

# Write the tables to S3
overview.write.mode('overwrite')\
.parquet("s3://ai4e-ap-southeast-1-dev-s3-data-landing/hauct/golden_zone/overview")

profit_customer.write.mode('overwrite')\
.parquet("s3://ai4e-ap-southeast-1-dev-s3-data-landing/hauct/golden_zone/profit_customer")

profit_distribute.write.mode('overwrite')\
.parquet("s3://ai4e-ap-southeast-1-dev-s3-data-landing/hauct/golden_zone/profit_distribute")