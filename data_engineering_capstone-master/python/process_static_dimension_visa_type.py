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

    # get visa-type mapping & write to dimension table & parquet on s3
    df = spark.read.csv(INPUT_PATH + "/mappings/i94_visa_types.txt", sep="=") \
        .toDF("id", "reason") \
        .withColumn("id", F.trim(F.col("id"))) \
        .withColumn("reason", F.trim(F.col("reason")))

    # show final table
    df.show()
    df.printSchema()

    # write to parquet on s3
    df.write.mode("overwrite").option("path", OUTPUT_PATH + "/dimension_visa_type/") \
        .saveAsTable(TABLE_SINK)

    df.write.format("com.databricks.spark.redshift") \
        .option("url", JDBC_URL) \
        .option("dbtable", TABLE_SINK) \
        .option("tempdir", TMP_DIR) \
        .mode("overwrite") \
        .save()

    spark.stop()
