import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
import pandas as pd
import numpy as np
import pandas_gbq
from pyspark.sql.types import DateType
from pyspark.sql.functions import pandas_udf, PandasUDFType
import os

# Create spark session
spark = (SparkSession
    .builder 
    .appName("spark-cleansing") 
    .getOrCreate()
    )
sc = spark.sparkContext
sc.setLogLevel("WARN")

####################################
# path file
####################################
csv_file = "/opt/airflow/bank_marketing.csv"

####################################
# Read csv Data
####################################
print("######################################")
print("READING CSV FILE")
print("######################################")

df = (
    spark.read
    .format("csv")
    .option("sep", ";")
    .option("header", True)
    .load(csv_file)
)

####################################
# Format Standarization
####################################
print("######################################")
print("FORMAT STANDARIZATION")
print("######################################")
# 1. Standardize the education value
df_transform1 = df.withColumn("education",
                                        when(df.education.endswith('4y'), regexp_replace(df.education, 'basic.4y', 'basic')) \
                                        .when(df.education.endswith('6y'), regexp_replace(df.education, 'basic.6y', 'basic')) \
                                        .when(df.education.endswith('9y'), regexp_replace(df.education, 'basic.9y', 'basic')) \
                                        .otherwise(df.education)
                                        )

# 2. Some column name need to be standardized because Spark & BigQuery can't read it
df_transform2 = df_transform1.withColumnRenamed('emp.var.rate', 'emp_var_rate') \
       .withColumnRenamed('cons.price.idx', 'cons_price_idx') \
       .withColumnRenamed('cons.conf.idx', 'cons_conf_idx') \
       .withColumnRenamed('nr.employed', 'nr_employed') \
       .withColumnRenamed('default', 'credit') \
       .withColumnRenamed('y', 'subscribed')

####################################
# Cleanse Null Data
####################################
print("######################################")
print("CLEANSE NULL DATA")
print("######################################")
df_transform3 = df_transform2.na.drop("any")

####################################
# Add client_id column and date column
####################################
print("######################################")
print("ADDING CLIENT_ID COLUMN")
print("######################################")
df_transform4 = df_transform3.withColumn("client_id", monotonically_increasing_id())
df_transform4 = df_transform4.select(["client_id"] + [col for col in df_transform4.columns if col != "client_id"])

print("######################################")
print("ADDING DATE COLUMN")
print("######################################")

"""# Define UDFs to map the month and day_of_week columns
month_dict = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6, 'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}
day_dict = {'mon': 1, 'tue': 2, 'wed': 3, 'thu': 4, 'fri': 5, 'sat': 6, 'sun': 7}  

udf_month = udf(lambda x: month_dict.get(x), IntegerType())
udf_day = udf(lambda x: day_dict.get(x), IntegerType())

df_transform4 = df_transform4.withColumn("month", udf_month(col("month")))
df_transform4 = df_transform4.withColumn("day_of_week", udf_day(col("day_of_week")))"""

####################################
# Save Data
####################################
print("######################################")
print("SAVE DATA")
print("######################################")

df_transform4.toPandas().to_csv("/opt/airflow/bank_marketing.csv", index=False)

print("######################################")
print("SUCCESS")
print("######################################")
