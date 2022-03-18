import argparse
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def initialize_spark_session():
    """
        Creates a spark-session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2,"
                                       "saurfang:spark-sas7bdat:2.1.0-s_2.11,"
                                       "com.databricks:spark-redshift_2.11:2.0.1") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def get_arguments():
    """
        Get custom command line arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="path pointing to input data (directory).")
    parser.add_argument("--output", help="path pointing to output directory.")
    parser.add_argument("--table-sink", help="name of the table-sink.")
    parser.add_argument("--jdbc-uri", help="jdbc-connection uri.")
    parser.add_argument("--tmp", help="path to store tmp data (directory).")

    return parser.parse_args()


def convert_latitude(x):
    """
        Converts latitude-coordinates based on North/East value.
    """
    direction = str(x)[-1]
    if direction == 'N':
        return float(str(x)[:-1])
    else:
        return -float(str(x)[:-1])


def convert_longitude(x):
    """
        Converts longitude-coordinates based on North/East value.
    """
    direction = str(x)[-1]
    if direction == 'E':
        return float(str(x)[:-1])
    else:
        return -float(str(x)[:-1])


def parse_latitude(x):
    """
        Parses latitude-coordinate out of a latitude/longitude-combination (separated by ',').
    """
    y = x.strip().split(',')
    return float(y[0])


def parse_longitude(x):
    """
        Parses longitude-coordinate out of a latitude/longitude-combination (separated by ',').
    """
    y = x.strip().split(',')
    return float(y[1])


def to_datetime_sas(x):
    """
        Creates a proper datetime-object out of a given day-based difference.
    """
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except (ValueError, TypeError):
        return None


def union_with_diff_columns(df1, df2):
    """
        Unions two data-frames with different columns.
    """
    col_df1 = df1.columns
    col_df2 = df2.columns
    total_columns = sorted(col_df1 + list(set(col_df2) - set(col_df1)))

    def expr(column_set, all_columns):
        def process_columns(column_name):
            if column_name in column_set:
                return column_name
            else:
                return F.lit(None).alias(column_name)

        cols = map(process_columns, all_columns)
        return list(cols)

    union_df = df1.select(expr(col_df1, total_columns)).union(df2.select(expr(col_df2, total_columns)))
    return union_df


def load_country_mapping(spark, file_path):
    """
        Loads the country-mapping into a Spark-Data-frame.
    """

    df = spark.read.csv(file_path, sep="=") \
        .toDF("id", "country") \
        .withColumn("id", F.regexp_replace(F.trim(F.col("id")), "'", "")) \
        .withColumn("country", F.regexp_replace(F.trim(F.col("country")), "'", ""))

    return df


def load_state_mapping(spark, file_path):
    """
        Loads the state-mapping into a Spark-Data-frame.
    """

    df = spark.read.csv(file_path, sep="=") \
        .toDF("id", "state") \
        .withColumn("id", F.regexp_replace(F.col("id"), "'", "")) \
        .withColumn("state", F.regexp_replace(F.col("state"), "'", ""))

    return df
