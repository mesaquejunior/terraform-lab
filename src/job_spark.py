from pyspark.sql.functions import col, udf, weekofyear, year
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession
from datetime import datetime

func = udf(lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())

spark = (
    SparkSession.builder.appName("ExerciseSpark")
    .getOrCreate()
)

mm_summary = (
    spark
    .read
    .format("csv")
    .option("header", False)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .load("s3://datalake-igti-mesaque/raw-data/MM_summary.txt")
)

mm_summary = (
    mm_summary
    .withColumnRenamed("_c0", "OPRODUCT_ID")
    .withColumnRenamed("_c1", "MEDIA_ID")
    .withColumnRenamed("_c2", "MARKET_ID")
    .withColumnRenamed("_c3", "PROPERTY_ID")
    .withColumnRenamed("_c4", "BROADCAST_WEEK")
    .withColumnRenamed("_c5", "LENGTH_V")
    .withColumnRenamed("_c6", "COST_V")
    .withColumnRenamed("_c7", "UNITS_V")
    .withColumnRenamed("_c8", "REP_MONTH")
    .withColumnRenamed("_c9", "CREATIVE_ID")
    .withColumnRenamed("_c10", "DIGITAL_IMP_V")
)

mm_summary = (
    mm_summary
    .withColumn('BROADCAST_WEEK', func(col('BROADCAST_WEEK')))
)

mm_summary = (
    mm_summary
    .withColumn('WEEK_OF_YEAR', weekofyear(col('BROADCAST_WEEK')))
    .withColumn('YEAR', year(col('BROADCAST_WEEK')))
)

(
    mm_summary
    .select("OPRODUCT_ID", "MEDIA_ID", "MARKET_ID", "PROPERTY_ID", "BROADCAST_WEEK", "LENGTH_V", "COST_V", "UNITS_V", "REP_MONTH", "CREATIVE_ID", "DIGITAL_IMP_V")
    .write
    .mode("overwrite")
    .format("parquet")
    .option("header", "false")
    .partitionBy("MEDIA_ID", "YEAR", "WEEK_OF_YEAR")
    .save("s3://datalake-igti-mesaque/staging/mm_summary")
)
