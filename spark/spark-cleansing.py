import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
import pandas as pd
import numpy as np
from pyspark.sql.types import DateType
from pyspark.sql.functions import pandas_udf, PandasUDFType,create_map, lit, col, to_date, concat,row_number
from itertools import chain
import os
from pyspark.sql.window import Window


def create_spark_session():
    """Create and return a SparkSession."""
    spark = (SparkSession.builder.appName("spark-cleansing").getOrCreate())
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    return spark


def load_data(spark, csv_file):
    """Load and return data from a csv file."""
    df = (
        spark.read
        .format("csv")
        .option("sep", ";")
        .option("header", True)
        .load(csv_file)
    )
    return df


def transform_data(df):
    """Transform the dataframe and return the transformed dataframe."""

    # Transform 'education' column to aggregate basic levels
    df = df.withColumn("education",
                    when(df.education.endswith('4y'), regexp_replace(df.education, 'basic.4y', 'basic')) \
                    .when(df.education.endswith('6y'), regexp_replace(df.education, 'basic.6y', 'basic')) \
                    .when(df.education.endswith('9y'), regexp_replace(df.education, 'basic.9y', 'basic')) \
                    .otherwise(df.education)
                    )

    # Rename columns to a more standardized format
    df = df.withColumnRenamed('emp.var.rate', 'emp_var_rate') \
    .withColumnRenamed('cons.price.idx', 'cons_price_idx') \
    .withColumnRenamed('cons.conf.idx', 'cons_conf_idx') \
    .withColumnRenamed('nr.employed', 'nr_employed') \
    .withColumnRenamed('default', 'credit') \
    .withColumnRenamed('y', 'subcribed')

    # Drop rows with any null values
    df = df.na.drop("any")

    # Add 'client_id' column as an index
    window = Window.orderBy(monotonically_increasing_id())
    df = df.withColumn("client_id", row_number().over(window) - 1)
    df = df.select(["client_id"] + [col for col in df.columns if col != "client_id"])

    return df


def update_with_dictionary(df):
    """Update the dataframe using dictionaries and return the updated dataframe."""

    # Check for 'unknown' values in 'month' and 'day_of_week'
    df.filter((df['month'].isNull()) | (df['day_of_week'].isNull())).show()
    df.filter((df['month'] == 'unknown') | (df['day_of_week'] == 'unknown')).show()

    # Updating dictionaries to match the values in your dataframe
    month_dict = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
                  "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}

    day_dict = {"mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5}

    subscribed_dict = {"no": 0, "yes": 1}

    # Use the dictionaries to map the month, day, and subscribed columns to numbers
    month_mapping_expr = create_map([lit(x) for x in chain(*month_dict.items())])
    day_mapping_expr = create_map([lit(x) for x in chain(*day_dict.items())])
    subscribed_mapping_expr = create_map([lit(x) for x in chain(*subscribed_dict.items())])

    df = df.withColumn('month', month_mapping_expr.getItem(col('month')))
    df = df.withColumn('day_of_week', day_mapping_expr.getItem(col('day_of_week')))
    df = df.withColumn('subcribed', subscribed_mapping_expr.getItem(col('subcribed')))

    return df


def create_date(df):
    """Create the date column and return the updated dataframe."""

    # Convert the month and day columns to string to avoid null values during concatenation
    df = df.withColumn('month', lpad(df['month'], 2, '0'))
    df = df.withColumn('day_of_week', lpad(df['day_of_week'], 2, '0'))

    # Create the date column
    df = df.withColumn("date", 
                       to_date(concat(lit("2022-"), 
                                      df["month"], 
                                      lit("-"),
                                      df["day_of_week"]), 
                                "yyyy-MM-dd"))

    return df


def save_to_csv(df, file_path):
    """Save the dataframe to a csv file."""
    df.toPandas().to_csv(file_path, index=False)


def main():
    spark = create_spark_session()
    csv_file = "/opt/airflow/bank_marketing.csv"
    df = load_data(spark, csv_file)
    df = transform_data(df)
    df = update_with_dictionary(df)
    df = create_date(df)
    save_to_csv(df, "/opt/airflow/bank_marketing.csv")


if __name__ == "__main__":
    main()
