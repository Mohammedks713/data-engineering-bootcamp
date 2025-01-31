from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from ..jobs.weekly_queries_job import user_devices_cumulated_query, hosts_cumulated_query
from collections import namedtuple

# Define data schema for tests
UserDevice = namedtuple("UserDevice", "user_id browser_type date_active")
UserDeviceCumulated = namedtuple("UserDeviceCumulated", "user_id browser_type device_activity_datelist date")
Host = namedtuple("Host", "host date_active")
HostCumulated = namedtuple("HostCumulated", "host host_activity_datelist date")


def test_user_devices_cumulated_query(spark):
    # Fake input data for today and yesterday
    today_data = [
        UserDevice(1, "Chrome", "2023-01-03"),
        UserDevice(2, "Firefox", "2023-01-03"),
    ]
    yesterday_data = [
        UserDeviceCumulated(1, "Chrome", ["2023-01-02"], "2023-01-02"),
    ]

    today_df = spark.createDataFrame(today_data)
    yesterday_df = spark.createDataFrame(yesterday_data)

    # Mock function to replace data loading
    def mock_user_devices_cumulated_query(spark):
        today_df.createOrReplaceTempView("events")
        yesterday_df.createOrReplaceTempView("yesterday")
        return user_devices_cumulated_query(spark)

    # Expected output data
    expected_data = [
        UserDeviceCumulated(1, "Chrome", ["2023-01-02", "2023-01-03"], "2023-01-03"),
        UserDeviceCumulated(2, "Firefox", ["2023-01-03"], "2023-01-03"),
    ]
    expected_df = spark.createDataFrame(expected_data)

    # Run the transformation
    actual_df = mock_user_devices_cumulated_query(spark)

    # Compare
    assert_df_equality(actual_df, expected_df)


def test_hosts_cumulated_query(spark):
    # Fake input data for today and yesterday
    today_data = [
        Host("host1", "2023-01-03"),
        Host("host2", "2023-01-03"),
    ]
    yesterday_data = [
        HostCumulated("host1", ["2023-01-02"], "2023-01-02"),
    ]

    today_df = spark.createDataFrame(today_data)
    yesterday_df = spark.createDataFrame(yesterday_data)

    # Mock function to replace data loading
    def mock_hosts_cumulated_query(spark):
        today_df.createOrReplaceTempView("events")
        yesterday_df.createOrReplaceTempView("yesterday")
        return hosts_cumulated_query(spark)

    # Expected output data
    expected_data = [
        HostCumulated("host1", ["2023-01-02", "2023-01-03"], "2023-01-03"),
        HostCumulated("host2", ["2023-01-03"], "2023-01-03"),
    ]
    expected_df = spark.createDataFrame(expected_data)

    # Run the transformation
    actual_df = mock_hosts_cumulated_query(spark)

    # Compare
    assert_df_equality(actual_df, expected_df)
