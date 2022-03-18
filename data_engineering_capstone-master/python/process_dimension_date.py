from pyspark.sql import functions as F
from python import get_arguments, initialize_spark_session


if __name__ == "__main__":
    """
        Spark Job to process raw data into date dimension-table.
    """
    args = get_arguments()

    # initialize spark-session
    spark = initialize_spark_session()

    JDBC_URL = args.jdbc_uri
    TABLE_SINK = args.table_sink
    OUTPUT_PATH = args.output + "/dimension_date/"
    TMP_DIR = args.tmp

    df = spark.sql("""SELECT * FROM stag_immigration""")

    # get date range
    df = df.select("arrival_date").distinct().orderBy("arrival_date")

    # generate columns
    df = df.withColumn("year", F.date_format("arrival_date", "y")) \
        .withColumn("month", F.date_format("arrival_date", "M")) \
        .withColumn("day", F.date_format("arrival_date", "d")) \
        .withColumn("month_string", F.date_format("arrival_date", "MMM")) \
        .withColumn("day_string", F.date_format("arrival_date", "E")) \
        .withColumn("week", F.date_format("arrival_date", "w")) \
        .withColumn("day_of_year", F.dayofyear("arrival_date")) \
        .withColumn("day_of_week", F.dayofweek("arrival_date")) \
        .withColumn("quarter", F.quarter("arrival_date"))

    # create unique identifier
    df = df.withColumn("id", F.monotonically_increasing_id() + 1)

    # select relevant columns
    df = df.select("id", "arrival_date", "year", "month", "day", "month_string",
                   "day_string", "week", "day_of_year", "day_of_week", "quarter")

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

