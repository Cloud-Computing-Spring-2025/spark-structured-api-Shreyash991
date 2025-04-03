from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import os
from datetime import datetime, timedelta
import shutil

def init_spark():
    """Initialize a Spark session"""
    return (SparkSession.builder
            .appName("Music Streaming Analytics")
            .getOrCreate())

def prepare_output_directory(base_dir="output"):
    """Create output directory structure, removing if it already exists"""
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)
        
    subdirs = [
        "user_favorite_genres",
        "avg_listen_time_per_song",
        "top_songs_this_week",
        "happy_recommendations",
        "genre_loyalty_scores",
        "night_owl_users",
        "enriched_logs"
    ]
    
    for subdir in subdirs:
        os.makedirs(os.path.join(base_dir, subdir), exist_ok=True)
        
    print(f"Created output directory structure in '{base_dir}'")
    
def load_data(spark):
    """Load the listening logs and songs metadata datasets"""
    # Define schemas
    logs_schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("song_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("duration_sec", IntegerType(), False)
    ])
    
    songs_schema = StructType([
        StructField("song_id", StringType(), False),
        StructField("title", StringType(), False),
        StructField("artist", StringType(), False),
        StructField("genre", StringType(), False),
        StructField("mood", StringType(), False)
    ])
    
    # Load data
    logs_df = (spark.read.schema(logs_schema)
              .option("header", "true")
              .csv("data/listening_logs.csv")
              .withColumn("timestamp", F.to_timestamp("timestamp")))
    
    songs_df = (spark.read.schema(songs_schema)
               .option("header", "true")
               .csv("data/songs_metadata.csv"))
    
    # Create a common dataframe by joining logs and metadata
    enriched_logs = logs_df.join(songs_df, on="song_id", how="inner")
    
    return logs_df, songs_df, enriched_logs

def task1_user_favorite_genres(enriched_logs):
    """Find each user's favorite genre by counting genre plays"""
    user_genre_counts = (enriched_logs
                        .groupBy("user_id", "genre")
                        .count()
                        .withColumnRenamed("count", "play_count"))
    
    # Create a window spec to find the max play count for each user
    window_spec = Window.partitionBy("user_id").orderBy(F.desc("play_count"))
    
    # Get the favorite genre for each user (the one with the highest play count)
    user_favorite_genres = (user_genre_counts
                           .withColumn("rank", F.row_number().over(window_spec))
                           .filter(F.col("rank") == 1)
                           .select("user_id", "genre", "play_count")
                           .orderBy("user_id"))
    
    user_favorite_genres.write.mode("overwrite").json("output/user_favorite_genres/")
    print("Task 1: User favorite genres saved to output/user_favorite_genres/")
    
    return user_favorite_genres

def task2_avg_listen_time_per_song(logs_df):
    """Calculate average listen time per song"""
    avg_listen_time = (logs_df
                      .groupBy("song_id")
                      .agg(
                          F.avg("duration_sec").alias("avg_duration_sec"),
                          F.count("*").alias("play_count")
                      )
                      .orderBy(F.desc("play_count")))
    
    avg_listen_time.write.mode("overwrite").json("output/avg_listen_time_per_song/")
    print("Task 2: Average listen time per song saved to output/avg_listen_time_per_song/")
    
    return avg_listen_time

def task3_top_songs_this_week(enriched_logs, songs_df):
    """List the top 10 most played songs this week"""
    # Calculate the start of the current week (last 7 days from now)
    current_date = datetime.now()
    one_week_ago = current_date - timedelta(days=7)
    
    # Filter logs for the current week and count plays per song
    top_songs = (enriched_logs
                .filter(F.col("timestamp") >= one_week_ago)
                .groupBy("song_id", "title", "artist")
                .count()
                .withColumnRenamed("count", "play_count")
                .orderBy(F.desc("play_count"))
                .limit(10))
    
    top_songs.write.mode("overwrite").json("output/top_songs_this_week/")
    print("Task 3: Top songs this week saved to output/top_songs_this_week/")
    
    return top_songs

def task4_happy_song_recommendations(enriched_logs, songs_df, spark):
    """Recommend 'Happy' songs to users who mostly listen to 'Sad' songs"""
    # Find users who primarily listen to sad songs
    mood_counts = (enriched_logs
                  .groupBy("user_id", "mood")
                  .count())
    
    window_spec = Window.partitionBy("user_id").orderBy(F.desc("count"))
    
    sad_song_listeners = (mood_counts
                         .withColumn("rank", F.row_number().over(window_spec))
                         .filter((F.col("rank") == 1) & (F.col("mood") == "Sad"))
                         .select("user_id"))
    
    # Find all songs user has listened to
    user_songs = (enriched_logs
                 .select("user_id", "song_id")
                 .distinct())
    
    # Find happy songs
    happy_songs = (songs_df
                  .filter(F.col("mood") == "Happy"))
    
    # For each user, find happy songs they haven't listened to yet
    recommendations = []
    
    for user_row in sad_song_listeners.collect():
        user_id = user_row["user_id"]
        
        # Get songs this user has already listened to
        user_listened_songs = (user_songs
                              .filter(F.col("user_id") == user_id)
                              .select("song_id")
                              .rdd.flatMap(lambda x: x).collect())
        
        # Find happy songs they haven't listened to
        user_recommendations = (happy_songs
                               .filter(~F.col("song_id").isin(user_listened_songs))
                               .orderBy(F.rand())  # Random selection
                               .limit(3)
                               .withColumn("user_id", F.lit(user_id))
                               .select("user_id", "song_id", "title", "artist"))
        
        recommendations.append(user_recommendations)
    
    # Combine all recommendations
    if recommendations:
        final_recommendations = recommendations[0]
        for i in range(1, len(recommendations)):
            final_recommendations = final_recommendations.union(recommendations[i])
    else:
        # Create empty DataFrame with appropriate schema if there are no recommendations
        final_recommendations = spark.createDataFrame([], 
                                                    StructType([
                                                        StructField("user_id", StringType(), True),
                                                        StructField("song_id", StringType(), True),
                                                        StructField("title", StringType(), True),
                                                        StructField("artist", StringType(), True)
                                                    ]))
    
    final_recommendations.write.mode("overwrite").json("output/happy_recommendations/")
    print("Task 4: Happy song recommendations saved to output/happy_recommendations/")
    
    return final_recommendations

def task5_genre_loyalty_scores(enriched_logs):
    """Compute genre loyalty score for each user"""
    # Count total plays per user
    user_total_plays = (enriched_logs
                       .groupBy("user_id")
                       .count()
                       .withColumnRenamed("count", "total_plays"))
    
    # Count plays per user per genre
    user_genre_plays = (enriched_logs
                       .groupBy("user_id", "genre")
                       .count()
                       .withColumnRenamed("count", "genre_plays"))
    
    # Find max genre plays for each user
    window_spec = Window.partitionBy("user_id")
    
    user_max_genre = (user_genre_plays
                     .withColumn("max_genre_plays", F.max("genre_plays").over(window_spec))
                     .filter(F.col("genre_plays") == F.col("max_genre_plays"))
                     .select("user_id", "genre", "max_genre_plays"))
    
    # Join with total plays and calculate loyalty score
    loyalty_scores = (user_max_genre.join(user_total_plays, on="user_id")
                     .withColumn("loyalty_score", F.col("max_genre_plays") / F.col("total_plays"))
                     .filter(F.col("loyalty_score") > 0.8)
                     .select("user_id", "genre", "loyalty_score")
                     .orderBy(F.desc("loyalty_score")))
    
    loyalty_scores.write.mode("overwrite").json("output/genre_loyalty_scores/")
    print("Task 5: Genre loyalty scores saved to output/genre_loyalty_scores/")
    
    return loyalty_scores

def task6_night_owl_users(logs_df):
    """Identify users who listen to music between 12 AM and 5 AM"""
    night_owl_users = (logs_df
                      .withColumn("hour", F.hour("timestamp"))
                      .filter((F.col("hour") >= 0) & (F.col("hour") < 5))
                      .groupBy("user_id")
                      .count()
                      .withColumnRenamed("count", "night_plays")
                      .orderBy(F.desc("night_plays")))
    
    night_owl_users.write.mode("overwrite").json("output/night_owl_users/")
    print("Task 6: Night owl users saved to output/night_owl_users/")
    
    return night_owl_users

def save_enriched_logs(enriched_logs):
    """Save the enriched logs for potential further analysis"""
    enriched_logs.write.mode("overwrite").parquet("output/enriched_logs/")
    print("Enriched logs saved to output/enriched_logs/")

if __name__ == "__main__":
    # Initialize Spark session
    spark = init_spark()
    
    # Prepare output directory
    prepare_output_directory()
    
    # Load data
    logs_df, songs_df, enriched_logs = load_data(spark)
    
    # Print some stats
    print(f"Loaded {logs_df.count()} listening logs")
    print(f"Loaded {songs_df.count()} songs")
    
    # Execute all tasks
    task1_user_favorite_genres(enriched_logs)
    task2_avg_listen_time_per_song(logs_df)
    task3_top_songs_this_week(enriched_logs, songs_df)
    task4_happy_song_recommendations(enriched_logs, songs_df, spark)
    task5_genre_loyalty_scores(enriched_logs)
    task6_night_owl_users(logs_df)
    save_enriched_logs(enriched_logs)
    
    print("All tasks completed successfully!")
