from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

from python import get_arguments, parse_latitude, parse_longitude, initialize_spark_session


if __name__ == "__main__":
    """
        Spark Job to ingest airport data into staging-table.
    """
    args = get_arguments()

    # initialize spark-session & register custom udf's
    spark = initialize_spark_session()
    udf_parse_latitude = F.udf(lambda x: parse_latitude(x), FloatType())
    udf_parse_longitude = F.udf(lambda x: parse_longitude(x), FloatType())

    INPUT_PATH = args.input + "/airport-codes_csv.csv"
    OUTPUT_PATH = args.output + "/staging_airport/"
    TABLE_SINK = args.table_sink

    # read data
    df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)

    # rename columns
    df = df.withColumnRenamed("ident", "icao_code")

    # split region column
    df = df.withColumn("iso_region", F.split(F.col("iso_region"), "-")[1])

    # transform latitude & longitude coordinates
    df = df.withColumn("latitude", udf_parse_latitude("coordinates")) \
        .withColumn("longitude", udf_parse_longitude("coordinates")) \
        .drop("coordinates")

    # show final table
    df.show()
    df.printSchema()

    # write to parquet on s3
    df.write.mode("overwrite").option("path", OUTPUT_PATH) \
        .saveAsTable(TABLE_SINK)

    spark.stop()

