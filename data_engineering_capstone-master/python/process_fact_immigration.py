from pyspark.sql import functions as F
from python import get_arguments, initialize_spark_session, load_state_mapping, load_country_mapping


if __name__ == "__main__":
    """
        Spark Job to process raw data into immigration fact-table.
    """
    args = get_arguments()

    # initialize spark-session
    spark = initialize_spark_session()

    JDBC_URL = args.jdbc_uri
    TABLE_SINK = args.table_sink
    INPUT_PATH = args.input
    OUTPUT_PATH = args.output + "/fact_immigration/"
    TMP_DIR = args.tmp

    df = spark.sql("""SELECT * FROM stag_immigration""")

    # derive new column indicating stayed-days
    df = df.withColumn("stayed_days", F.datediff("departure_date", "arrival_date"))

    # get us-states mapping & join to get only valid i94-us-states, missing states are set to '99'
    state_mapping = load_state_mapping(spark, INPUT_PATH + "/mappings/i94_states.txt")
    df = df.join(state_mapping, df.i94_address == state_mapping.id, "left") \
        .fillna({"id": "99"}) \
        .drop("state", "i94_address") \
        .withColumnRenamed("id", "state_id")

    # get modes mapping & join to get only valid i94-modes, missing values are set to 'Not reported'
    mode_mapping = spark.sql("""SELECT * FROM dim_travel_mode""")
    df = df.join(mode_mapping, df.i94_mode == mode_mapping.id, "left") \
        .fillna({"id": "Not reported"}) \
        .drop("transport", "i94_mode") \
        .withColumnRenamed("id", "mode_id")

    # get visa mapping & join to get only valid i94-visa-types, missing values are set to 'Other'
    visa_mapping = spark.sql("""SELECT * FROM dim_visa_type""")
    df = df.join(visa_mapping, df.i94_visa_code == visa_mapping.id, "left") \
        .fillna({"id": "Other"}) \
        .drop("reason", "i94_visa_code") \
        .withColumnRenamed("id", "visa_id")

    # get port mapping & join to get only valid i94-ports
    port_mapping = spark.sql("""SELECT * FROM dim_port""").drop("state_id")
    df = df.join(port_mapping, df.i94_port == port_mapping.id, "left") \
        .drop("city", "i94_port") \
        .withColumnRenamed("id", "port_id")

    # get country mapping & join to get only valid i94 residences
    country_mapping = load_country_mapping(spark, INPUT_PATH + "/mappings/i94_city_res.txt")
    df = df.join(country_mapping, df.i94_residence == country_mapping.id, "left") \
        .drop("country", "i94_residence") \
        .withColumnRenamed("id", "country_id")

    # create unique identifier
    df = df.withColumn("id", F.monotonically_increasing_id() + 1)

    # select relevant columns
    df = df.select("id", "admission_number", "cic_id", "ins_number", "i94_year", "i94_month", "arrival_date",
                   "departure_date", "stayed_days", "airline", "flight_number", "gender", "i94_age", "year_of_birth",
                   "occupation", "i94_city", "country_id", "state_id", "port_id", "mode_id", "visa_id", "visa_type")

    # show final table
    df.show()
    df.printSchema()

    # write to parquet on s3
    df.write.mode("overwrite").option("path", OUTPUT_PATH) \
        .saveAsTable(TABLE_SINK)

    # save final fact table
    df.write.format("com.databricks.spark.redshift") \
        .option("url", JDBC_URL) \
        .option("dbtable", TABLE_SINK) \
        .option("tempdir", TMP_DIR) \
        .mode("append") \
        .save()

    spark.stop()

