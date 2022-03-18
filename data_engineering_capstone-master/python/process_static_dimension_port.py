from pyspark.sql import functions as F
from python import get_arguments, initialize_spark_session


if __name__ == "__main__":
    """
        Spark Job to process raw data mappings to static dimension tables.
    """
    args = get_arguments()

    # initialize spark-session
    spark = initialize_spark_session()

    JDBC_URL = args.jdbc_uri
    TABLE_SINK = args.table_sink
    TMP_DIR = args.tmp
    INPUT_PATH = args.input
    OUTPUT_PATH = args.output

    # get port mapping & write to dimension table & parquet on s3
    df = spark.read.csv(INPUT_PATH + "/mappings/i94_ports.txt", sep="=") \
        .toDF("id", "city") \
        .withColumn("id", F.regexp_replace(F.col("id"), "'", "")) \
        .withColumn("state_id", F.trim(F.regexp_replace(F.split("city", ",")[1], "'", ""))) \
        .withColumn("city", F.trim(F.regexp_replace(F.split("city", ",")[0], "'", "")))

    # show final table
    df.show()
    df.printSchema()

    # write to parquet on s3
    df.write.mode("overwrite").option("path", OUTPUT_PATH + "/dimension_port/") \
        .saveAsTable(TABLE_SINK)

    df.write.format("com.databricks.spark.redshift") \
        .option("url", JDBC_URL) \
        .option("dbtable", TABLE_SINK) \
        .option("tempdir", TMP_DIR) \
        .mode("overwrite") \
        .save()

    spark.stop()
