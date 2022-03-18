from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

from python import get_arguments, convert_latitude, convert_longitude, initialize_spark_session


if __name__ == "__main__":
    """
        Spark Job to ingest temperature data into staging-table.
    """
    args = get_arguments()

    # initialize spark-session & register custom udf's
    spark = initialize_spark_session()
    udf_convert_latitude = F.udf(lambda x: convert_latitude(x), FloatType())
    udf_convert_longitude = F.udf(lambda x: convert_longitude(x), FloatType())

    INPUT_PATH = args.input + "/GlobalLandTemperaturesByCity.csv"
    OUTPUT_PATH = args.output + "/staging_temperature/"
    TABLE_SINK = args.table_sink

    # read data
    df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)

    # rename columns
    df = df.withColumnRenamed("dt", "timestamp") \
        .withColumnRenamed("AverageTemperature", "average_temperature") \
        .withColumnRenamed("AverageTemperatureUncertainty", "average_temperature_uncertainty") \
        .withColumnRenamed("City", "city") \
        .withColumnRenamed("Country", "country") \
        .withColumnRenamed("Latitude", "latitude") \
        .withColumnRenamed("Longitude", "longitude")

    # transform latitude & longitude coordinates
    df = df.withColumn("latitude", udf_convert_latitude("latitude")) \
        .withColumn("longitude", udf_convert_longitude("longitude"))

    # show final table
    df.show()
    df.printSchema()

    # write to parquet on s3
    df.write.mode("overwrite").option("path", OUTPUT_PATH) \
        .saveAsTable(TABLE_SINK)

    spark.stop()

