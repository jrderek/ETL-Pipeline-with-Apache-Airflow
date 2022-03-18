from pyspark.sql import functions as F
from python import get_arguments, initialize_spark_session, load_state_mapping


if __name__ == "__main__":
    """
        Spark Job to process raw data into state dimension-table.
    """
    args = get_arguments()

    # initialize spark-session
    spark = initialize_spark_session()

    JDBC_URL = args.jdbc_uri
    TABLE_SINK = args.table_sink
    INPUT_PATH = args.input
    OUTPUT_PATH = args.output + "/dimension_state/"
    TMP_DIR = args.tmp

    df = spark.sql("""SELECT * FROM stag_demographic""")

    # clean dataframe based on relevant columns
    df = df.filter(df.city.isNotNull() | df.state.isNotNull() | df.race.isNotNull()) \
        .dropDuplicates(subset=["city", "state", "race"])

    # pivot race and population count & combine as separate columns
    race_pivoted_df = df.select("city", "state", "state_code", "race", "count") \
        .groupby("city", "state", "state_code") \
        .pivot("race") \
        .agg(F.first("count")) \
        .withColumnRenamed("American Indian and Alaska Native", "american_indian_alaska_native") \
        .withColumnRenamed("Asian", "asian") \
        .withColumnRenamed("Black or African-American", "african_american") \
        .withColumnRenamed("Hispanic or Latino", "hispanic_latino") \
        .withColumnRenamed("White", "white").fillna(
        {"american_indian_alaska_native": 0, "asian": 0, "african_american": 0, "hispanic_latino": 0, "white": 0})

    df = df.join(race_pivoted_df, ["city", "state", "state_code"]).drop("race", "count")

    df = df.groupby(["state", "state_code"]) \
        .agg(F.sum("male_population").alias("male_population"), F.sum("female_population").alias("female_population"),
             F.sum("total_population").alias("total_population"),
             F.sum("num_veterans").alias("num_veterans"), F.sum("foreign_born").alias("foreign_born"),
             F.sum("american_indian_alaska_native").alias("american_indian_alaska_native"),
             F.sum("asian").alias("asian"), F.sum("african_american").alias("african_american"),
             F.sum("hispanic_latino").alias("hispanic_latino"), F.sum("white").alias("white")) \
        .withColumnRenamed("state_code", "state_id")

    # get us-states mapping
    state_mapping = load_state_mapping(spark, INPUT_PATH + "/mappings/i94_states.txt").drop("state")

    # join dataframe with state-mapping to get only valid i94-us-states, missing states are set to '99'
    df = df.join(state_mapping, df.state_id == state_mapping.id, "right") \
        .fillna({"id": "99"}) \
        .drop("state_id") \
        .withColumnRenamed("id", "state_id")

    # create unique identifier
    df = df.withColumn("id", F.monotonically_increasing_id() + 1)

    # select relevant columns
    df = df.select("id", "state_id", "state", "male_population", "female_population", "total_population",
                   "num_veterans", "foreign_born", "american_indian_alaska_native", "asian", "african_american",
                   "hispanic_latino", "white")

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

