from pyspark.sql import functions as F
from python import get_arguments, initialize_spark_session


if __name__ == "__main__":
    """
        Spark Job to process raw data into airport dimension-table.
    """
    args = get_arguments()

    # initialize spark-session
    spark = initialize_spark_session()

    JDBC_URL = args.jdbc_uri
    TABLE_SINK = args.table_sink
    OUTPUT_PATH = args.output + "/dimension_airport/"
    TMP_DIR = args.tmp

    df = spark.sql("""SELECT * FROM stag_airport""")

    # get airport_code column (key to fact table) as combination of iata_code & local_code
    df = df.drop("local_code").filter("iata_code is not null").withColumnRenamed("iata_code", "airport_code") \
        .union(df.drop("iata_code").filter("local_code is not null").withColumnRenamed("local_code", "airport_code")) \
        .distinct()

    # rename columns
    df = df.withColumnRenamed("iso_region", "state_id") \
        .withColumnRenamed("iso_country", "country")

    # drop unused columns
    df = df.drop("continent")

    # create unique identifier
    df = df.withColumn("id", F.monotonically_increasing_id() + 1)

    # select relevant columns
    df = df.select("id", "icao_code", "airport_code", "state_id", "country", "name", "type",
                   "municipality", "elevation_ft", "gps_code", "latitude", "longitude")

    # show final table
    df.show()
    df.printSchema()

    # write to parquet on s3
    df.write.mode("overwrite").option("path", OUTPUT_PATH) \
        .saveAsTable(TABLE_SINK)

    # save final dimension table
    df.write.format("com.databricks.spark.redshift") \
        .option("url", JDBC_URL) \
        .option("dbtable", TABLE_SINK) \
        .option("tempdir", TMP_DIR) \
        .mode("append") \
        .save()

    spark.stop()

