from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, sum, countDistinct

def create_spark_session():
    """Create and configure the Spark session."""
    return SparkSession.builder \
        .appName("Halo Analysis") \
        .master("local[*]") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .getOrCreate()

def load_dataframes(spark, paths):
    """Load all required CSV files into Spark DataFrames."""
    medals_df = spark.read.csv(paths['medals'], header=True, inferSchema=True)
    maps_df = spark.read.csv(paths['maps'], header=True, inferSchema=True)
    matches_df = spark.read.csv(paths['matches'], header=True, inferSchema=True)
    match_details_df = spark.read.csv(paths['match_details'], header=True, inferSchema=True)
    medals_matches_players_df = spark.read.csv(paths['medals_matches_players'], header=True, inferSchema=True)

    # Handle nulls
    medals_matches_players_df = medals_matches_players_df.na.fill({"count": 0})
    match_details_df = match_details_df.na.fill(0)

    return medals_df, maps_df, matches_df, match_details_df, medals_matches_players_df

def broadcast_joins(medals_df, maps_df, medals_matches_players_df, matches_df):
    """Broadcast join medals and maps with their respective datasets."""

    # Join medals with medals_matches_players
    medals_with_players = medals_matches_players_df.join(
        broadcast(medals_df), "medal_id", "inner"
    )

    # Join maps with matches
    maps_with_matches = matches_df.join(
        broadcast(maps_df), "mapid", "inner"
    )

    return medals_with_players, maps_with_matches

def write_bucketed_tables(match_details_df, maps_with_matches, medals_with_players):
    """Write bucketed tables to disk for optimized joins."""
    match_details_df.write.bucketBy(16, "match_id").sortBy("match_id").mode("overwrite").saveAsTable("bucketed_match_details")
    maps_with_matches.write.bucketBy(16, "match_id").sortBy("match_id").mode("overwrite").saveAsTable("bucketed_matches")
    medals_with_players.write.bucketBy(16, "match_id").sortBy("match_id").mode("overwrite").saveAsTable("bucketed_medals_matches_players")

def bucketed_joins(spark):
    """Perform bucketed joins on match_id."""
    match_details_bucketed = spark.table("bucketed_match_details")
    matches_bucketed = spark.table("bucketed_matches")
    medals_matches_players_bucketed = spark.table("bucketed_medals_matches_players")

    return match_details_bucketed.join(
        matches_bucketed, "match_id", "inner"
    ).join(
        medals_matches_players_bucketed, "match_id", "inner"
    )

def aggregate_data(bucketed_joins_df, medals_with_maps_df):
    """Aggregate data to answer the required questions."""
    # 1. Which player averages the most kills per game?
    player_kills_avg = bucketed_joins_df.groupBy("player_gamertag").agg(
        (sum("player_total_kills") / countDistinct("match_id")).alias("avg_kills_per_game")
    ).orderBy("avg_kills_per_game", ascending=False)

    # 2. Which playlist gets played the most?
    playlist_most_played = bucketed_joins_df.groupBy("playlist_id").count().orderBy("count", ascending=False)

    # 3. Which map gets played the most?
    map_most_played = bucketed_joins_df.groupBy("mapid").count().orderBy("count", ascending=False)

    # 4. Which map do players get the most Killing Spree medals on?
    killing_spree_medals = medals_with_maps_df.filter(medals_with_maps_df["name"] == "Killing Spree")
    most_killing_spree_map = killing_spree_medals.groupBy("mapid").agg(
        sum("count").alias("total_killing_spree")
    ).orderBy("total_killing_spree", ascending=False)

    return player_kills_avg, playlist_most_played, map_most_played, most_killing_spree_map

def experiment_with_sort(bucketed_joins_df):
    """Try different .sortWithinPartitions and partitioning strategies."""
    sorted_by_playlist = bucketed_joins_df.sortWithinPartitions("playlist_id")
    sorted_by_map = bucketed_joins_df.sortWithinPartitions("mapid")

    # Additional partitioning strategies
    partitioned_by_player = bucketed_joins_df.repartition(16, "player_gamertag")
    sorted_by_player_and_map = partitioned_by_player.sortWithinPartitions("player_gamertag", "mapid")

    return sorted_by_playlist, sorted_by_map, sorted_by_player_and_map

def compare_partitioning_strategies(bucketed_joins_df):
    """Compare performance metrics for different partitioning strategies."""
    from time import time

    strategies = {
        "Sort by Playlist": bucketed_joins_df.sortWithinPartitions("playlist_id"),
        "Sort by Map": bucketed_joins_df.sortWithinPartitions("mapid"),
        "Partition by Player and Sort by Player and Map": bucketed_joins_df.repartition(16, "player_gamertag").sortWithinPartitions("player_gamertag", "mapid")
    }

    performance_metrics = {}

    for strategy_name, df in strategies.items():
        start_time = time()
        df.count()  # Trigger an action to measure performance
        end_time = time()
        performance_metrics[strategy_name] = end_time - start_time

    return performance_metrics

def main():
    # Define paths to data
    paths = {
        'medals': "path_to/medals.csv",
        'maps': "path_to/maps.csv",
        'matches': "path_to/matches.csv",
        'match_details': "path_to/match_details.csv",
        'medals_matches_players': "path_to/medals_matches_players.csv"
    }

    # Initialize Spark session
    spark = create_spark_session()

    # Load data
    medals_df, maps_df, matches_df, match_details_df, medals_matches_players_df = load_dataframes(spark, paths)

    # Perform joins
    medals_with_players, maps_with_matches = broadcast_joins(medals_df, maps_df, medals_matches_players_df, matches_df)

    # Write bucketed tables to disk
    write_bucketed_tables(match_details_df, maps_with_matches, medals_with_players)

    # Perform bucketed joins
    bucketed_joins_df = bucketed_joins(spark)

    # Aggregate data
    player_kills_avg, playlist_most_played, map_most_played, most_killing_spree_map = aggregate_data(
        bucketed_joins_df, maps_with_matches
    )

    # Experiment with sorting
    sorted_by_playlist, sorted_by_map, sorted_by_player_and_map = experiment_with_sort(bucketed_joins_df)

    # Compare partitioning strategies
    performance_metrics = compare_partitioning_strategies(bucketed_joins_df)

    # Show results (example)
    player_kills_avg.show()
    playlist_most_played.show()
    map_most_played.show()
    most_killing_spree_map.show()

    print("Performance Metrics:")
    for strategy, time_taken in performance_metrics.items():
        print(f"{strategy}: {time_taken:.2f} seconds")

    spark.stop()

if __name__ == "__main__":
    main()


