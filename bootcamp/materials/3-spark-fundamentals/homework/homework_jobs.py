from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def create_spark_session():
    return SparkSession.builder \
        .appName("Weekly Queries Conversion") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .getOrCreate()

def user_devices_cumulated_query(spark):
    # Load data
    events = spark.read.parquet("path_to_events")
    devices = spark.read.parquet("path_to_devices")

    # Filter for the given date (this_date) and group by user_id and browser_type
    today = events \
        .join(devices, "device_id") \
        .withColumn("date_active", F.to_date("event_time")) \
        .filter((F.col("event_time").isNotNull()) & (F.col("user_id").isNotNull())) \
        .groupby("user_id", "browser_type", "date_active") \
        .count()

    yesterday = spark.read.parquet("path_to_yesterday_user_devices")

    # Perform the FULL OUTER JOIN and create the cumulated device activity datelist
    result = today.alias("t") \
        .join(yesterday.alias("y"), (F.col("t.user_id") == F.col("y.user_id")) &
             (F.col("t.browser_type") == F.col("y.browser_type")), "full_outer") \
        .select(
            F.coalesce(F.col("t.user_id"), F.col("y.user_id")).alias("user_id"),
            F.coalesce(F.col("t.browser_type"), F.col("y.browser_type")).alias("browser_type"),
            F.when(F.col("y.device_activity_datelist").isNull(), F.array(F.col("t.date_active")))
            .when(F.col("t.date_active").isNull(), F.col("y.device_activity_datelist"))
            .otherwise(F.array_union(F.array(F.col("t.date_active")), F.col("y.device_activity_datelist"))).alias("device_activity_datelist"),
            F.lit("2023-01-03").alias("date")
        )

    result.write.parquet("path_to_output_user_devices_cumulated")

def hosts_cumulated_query(spark):
    # Load data
    events = spark.read.parquet("path_to_events")
    yesterday = spark.read.parquet("path_to_yesterday_hosts")

    # Filter for the given date and group by host
    today = events \
        .withColumn("date_active", F.to_date("event_time")) \
        .filter(F.col("event_time").isNotNull()) \
        .groupby("host", "date_active") \
        .count()

    # Perform the FULL OUTER JOIN and create the cumulated host activity datelist
    result = today.alias("t") \
        .join(yesterday.alias("y"), F.col("t.host") == F.col("y.host"), "full_outer") \
        .select(
            F.coalesce(F.col("t.host"), F.col("y.host")).alias("host"),
            F.when(F.col("y.host_activity_datelist").isNull(), F.array(F.col("t.date_active")))
            .when(F.col("t.date_active").isNull(), F.col("y.host_activity_datelist"))
            .otherwise(F.array_union(F.array(F.col("t.date_active")), F.col("y.host_activity_datelist"))).alias("host_activity_datelist"),
            F.lit("2023-01-03").alias("date")
        )

    result.write.parquet("path_to_output_hosts_cumulated")

def main():
    spark = create_spark_session()

    # Execute the converted queries
    user_devices_cumulated_query(spark)
    hosts_cumulated_query(spark)

if __name__ == "__main__":
    main()
