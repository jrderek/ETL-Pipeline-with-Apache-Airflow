from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, LongType

from python import get_arguments, parse_latitude, parse_longitude, initialize_spark_session


if __name__ == "__main__":
    """
        Spark Job to ingest demographic data into staging-table.
    """
    args = get_arguments()

    # initialize spark-session & register custom udf's
    spark = initialize_spark_session()
    udf_parse_latitude = F.udf(lambda x: parse_latitude(x), FloatType())
    udf_parse_longitude = F.udf(lambda x: parse_longitude(x), FloatType())

    INPUT_PATH = args.input + "/us-cities-demographics.csv"
    OUTPUT_PATH = args.output + "/staging_demographic/"
    TABLE_SINK = args.table_sink

    # read data
    df = spark.read.csv(INPUT_PATH, sep=";", header=True, inferSchema=True)

    # rename columns
    df = df.withColumnRenamed("Average Household Size", "avg_household_size") \
        .withColumnRenamed("State Code", "state_code") \
        .withColumnRenamed("Race", "race") \
        .withColumnRenamed("Median Age", "median_age") \
        .withColumnRenamed("City", "city") \
        .withColumnRenamed("State", "state") \
        .withColumnRenamed("Count", "count")

    # cast & drop columns
    df = df.withColumn("male_population", F.col("Male Population").cast(LongType())) \
        .withColumn("female_population", F.col("Female Population").cast(LongType())) \
        .withColumn("total_population", F.col("Total Population").cast(LongType())) \
        .withColumn("num_veterans", F.col("Number of Veterans").cast(LongType())) \
        .withColumn("foreign_born", F.col("Foreign-born").cast(LongType())) \
        .drop("Male Population", "Female Population", "Total Population", "Number of Veterans", "Foreign-born")

    # show final table
    df.show()
    df.printSchema()

    # write to parquet on s3
    df.write.mode("overwrite").option("path", OUTPUT_PATH) \
        .saveAsTable(TABLE_SINK)

    spark.stop()

