import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import lit, col, session
import psycopg2
from psycopg2 import sql


def create_aggregated_events_sink_postgres(t_env):
    """Creates a PostgreSQL sink table for storing aggregated session data."""
    table_name = 'processed_events_aggregated'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            ip VARCHAR,
            host VARCHAR,
            num_events BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name




def create_processed_events_source_kafka(t_env):
    """Creates a Kafka source table to ingest web event data."""
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "process_events_kafka"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND
        ) WITH (
             'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def log_aggregation():
    """Main function to run the Flink job."""
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka source table
        source_table = create_processed_events_source_kafka(t_env)

        # Create PostgreSQL sink table
        aggregated_table = create_aggregated_events_sink_postgres(t_env)

        # Sessionize and aggregate data
        sessionized_table = t_env.from_path(source_table) \
            .window(
            session(lit(5).minutes).on(col("window_timestamp")).by(col("ip")).alias("session_window")
        ) \
            .group_by(
            col("session_window"),
            col("ip"),
            col("host")
        ) \
            .select(
            col("session_window").start.alias("session_start"),
            col("session_window").end.alias("session_end"),
            col("ip"),
            col("host"),
            col("num_events").count.alias("num_events")
        )

        # Write sessionized data to PostgreSQL
        sessionized_table.execute_insert(aggregated_table).wait()

        print("Data ingestion and sessionization complete.")
    except KeyError as e:
        print(f"Missing environment variable: {str(e)}")
    except (ValueError, TypeError) as e:
        print(f"Data transformation error: {str(e)}")
    except psycopg2.Error as e:
        print(f"Database error: {str(e)}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")


if __name__ == '__main__':
    log_aggregation()