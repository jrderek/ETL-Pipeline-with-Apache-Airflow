from pyspark.sql import functions as F
from python import get_arguments, initialize_spark_session, load_country_mapping


if __name__ == "__main__":
    """
        Spark Job to process raw data into country dimension-table.
    """
    args = get_arguments()

    # initialize spark-session
    spark = initialize_spark_session()

    JDBC_URL = args.jdbc_uri
    TABLE_SINK = args.table_sink
    INPUT_PATH = args.input
    OUTPUT_PATH = args.output + "/dimension_country/"
    TMP_DIR = args.tmp

    df = spark.sql("""SELECT * FROM stag_temperature""")

    # clean data
    df = df.filter("average_temperature IS NOT NULL")

    # aggregate dataframe by country
    df = df.groupBy("country") \
        .agg(F.avg("average_temperature").alias("average_temperature"),
             F.avg("average_temperature_uncertainty").alias("average_temperature_uncertainty"),
             F.first("latitude").alias("latitude"), F.first("longitude").alias("longitude"))

    # rename column values for better matching on join
    df = df.withColumn("country", F.when(F.col("country") == "Congo (Democratic Republic Of The)", "Congo")
                       .otherwise(F.col("country"))) \
        .withColumn("country",  F.when(F.col("country") == "Côte D'Ivoire", "Ivory Coast")
                    .otherwise(F.col("country"))) \
        .withColumnRenamed("country", "cntry")

    # get country-mapping & rename column values for better matching on join
    country_mapping = load_country_mapping(spark, INPUT_PATH + "/mappings/i94_city_res.txt") \
        .withColumn("country", F.when(F.col("country") == "BOSNIA-HERZEGOVINA", "BOSNIA AND HERZEGOVINA")
                    .otherwise(F.col("country"))) \
        .withColumn("country", F.when(F.col("country") == "INVALID: CANADA", "CANADA")
                    .otherwise(F.col("country"))) \
        .withColumn("country", F.when(F.col("country") == "CHINA, PRC", "CHINA")
                    .otherwise(F.col("country"))) \
        .withColumn("country",  F.when(F.col("country") == "GUINEA-BISSAU", "GUINEA BISSAU")
                    .otherwise(F.col("country"))) \
        .withColumn("country",  F.when(F.col("country") == "INVALID: PUERTO RICO", "PUERTO RICO")
                    .otherwise(F.col("country"))) \
        .withColumn("country",  F.when(F.col("country") == "INVALID: UNITED STATES", "UNITED STATES")
                    .otherwise(F.col("country")))

    # join dataframe with country-mapping
    df = df.join(country_mapping, F.lower(df.cntry) == F.lower(country_mapping.country), "right") \
        .drop("cntry") \
        .withColumnRenamed("id", "country_id")

    # create unique identifier
    df = df.withColumn("id", F.monotonically_increasing_id() + 1)

    # select relevant columns
    df = df.select("id", "country_id", "country", "average_temperature", "average_temperature_uncertainty",
                   "latitude", "longitude")

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

