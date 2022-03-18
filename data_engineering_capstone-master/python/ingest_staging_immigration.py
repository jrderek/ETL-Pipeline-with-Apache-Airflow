from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, LongType, IntegerType, DateType

from python import get_arguments, to_datetime_sas, union_with_diff_columns, initialize_spark_session


if __name__ == "__main__":
    """
        Spark Job to ingest immigration data into staging-table.
    """
    args = get_arguments()

    # initialize spark-session & register custom udf's
    spark = initialize_spark_session()
    udf_to_datetime_sas = F.udf(lambda x: to_datetime_sas(x), DateType())

    INPUT_PATH = args.input + "/18-83510-I94-Data-2016/"
    OUTPUT_PATH = args.output + "/staging_immigration/"
    TABLE_SINK = args.table_sink
    MONTH_YEAR = ["jan16", "feb16", "mar16", "apr16", "may16", "jun16",
                  "jul16", "aug16", "sep16", "oct16", "nov16", "dec16"]

    # read all year sas-data
    for idx, my in enumerate(MONTH_YEAR):
        sample = spark.read.format("com.github.saurfang.sas.spark").load(INPUT_PATH + "i94_{}_sub.sas7bdat".format(my))
        if idx == 0:
            df = sample
        else:
            df = union_with_diff_columns(df, sample)

    # rename columns
    df = df.withColumnRenamed("cicid", "cic_id") \
        .withColumnRenamed("i94yr", "i94_year") \
        .withColumnRenamed("i94mon", "i94_month") \
        .withColumnRenamed("i94cit", "i94_city") \
        .withColumnRenamed("i94res", "i94_residence") \
        .withColumnRenamed("i94port", "i94_port") \
        .withColumnRenamed("arrdate", "arrival_date") \
        .withColumnRenamed("i94mode", "i94_mode") \
        .withColumnRenamed("i94addr", "i94_address") \
        .withColumnRenamed("depdate", "departure_date") \
        .withColumnRenamed("i94bir", "i94_age") \
        .withColumnRenamed("i94visa", "i94_visa_code") \
        .withColumnRenamed("count", "count") \
        .withColumnRenamed("dtadfile", "date_file_added") \
        .withColumnRenamed("visapost", "visa_issued_by_department") \
        .withColumnRenamed("occup", "occupation") \
        .withColumnRenamed("entdepa", "arrival_flag") \
        .withColumnRenamed("entdepd", "departure_flag") \
        .withColumnRenamed("entdepu", "update_flag") \
        .withColumnRenamed("matflag", "arr_dep_match_flag") \
        .withColumnRenamed("biryear", "year_of_birth") \
        .withColumnRenamed("dtaddto", "date_admitted") \
        .withColumnRenamed("insnum", "ins_number") \
        .withColumnRenamed("admnum", "admission_number") \
        .withColumnRenamed("fltno", "flight_number") \
        .withColumnRenamed("visatype", "visa_type") \
        .withColumnRenamed("validres", "valid_resident")

    # transform columns
    df = df.withColumn("arrival_date", udf_to_datetime_sas("arrival_date")) \
        .withColumn("departure_date", udf_to_datetime_sas("departure_date")) \
        .withColumn("date_admitted", F.to_date("date_admitted", "MMddyyyy")) \
        .withColumn("date_file_added", F.to_date("date_file_added", "yyyyMMdd"))

    # cast columns
    df = df.withColumn("cic_id", F.col("cic_id").cast(IntegerType())) \
        .withColumn("admission_number", F.col("admission_number").cast(IntegerType())) \
        .withColumn("year_of_birth", F.col("year_of_birth").cast(IntegerType())) \
        .withColumn("i94_age", F.col("i94_age").cast(IntegerType())) \
        .withColumn("i94_city", F.col("i94_city").cast(IntegerType())) \
        .withColumn("i94_mode", F.col("i94_mode").cast(IntegerType())) \
        .withColumn("i94_month", F.col("i94_month").cast(IntegerType())) \
        .withColumn("i94_residence", F.col("i94_residence").cast(IntegerType())) \
        .withColumn("i94_visa_code", F.col("i94_visa_code").cast(IntegerType())) \
        .withColumn("i94_year", F.col("i94_year").cast(IntegerType()))

    # show final table
    df.show()
    df.printSchema()

    # write to parquet on s3
    df.write.mode("overwrite").option("path", OUTPUT_PATH) \
        .saveAsTable(TABLE_SINK)

    spark.stop()

