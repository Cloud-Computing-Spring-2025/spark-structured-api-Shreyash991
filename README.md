# Music Streaming Analytics with Spark Structured API

## How to Run

To run the analysis:

1. Ensure you have Python and Spark installed
2. Generate the datasets:
```bash
python data_generator.py
```

3. Run the Spark analysis:
```bash
spark-submit music_analytics.py
```

## Overview
This project implements analytics on music streaming data using Apache Spark's Structured APIs. It analyzes user listening behavior and music trends from data of a fictional music streaming platform to gain insights into genre preferences, song popularity, and listener engagement.

## Datasets Description

The project uses two datasets:

1. **listening_logs.csv**
   - Contains log data capturing each user's listening activity
   - Schema:
     - `user_id`: Unique ID of the user
     - `song_id`: Unique ID of the song
     - `timestamp`: Date and time when the song was played
     - `duration_sec`: Duration in seconds for which the song was played

2. **songs_metadata.csv**
   - Contains metadata about the songs in the catalog
   - Schema:
     - `song_id`: Unique ID of the song
     - `title`: Title of the song
     - `artist`: Name of the artist
     - `genre`: Genre of the song (e.g., Pop, Rock, Jazz)
     - `mood`: Mood category of the song (e.g., Happy, Sad, Energetic, Chill)

## Implementation

The implementation consists of two main components:

1. **Data Generation**: `data_generator.py` generates realistic sample data for both datasets
2. **Analysis**: `music_analytics.py` implements all the required analyses using Spark Structured APIs

## Analytics Tasks

### 1. Find each user's favorite genre
Identifies the most listened-to genre for each user by counting how many times they played songs in each genre.

Results saved to: `output/user_favorite_genres/`

Sample output:
```json
{"user_id":"U0001","genre":"R&B","play_count":57}
{"user_id":"U0002","genre":"Jazz","play_count":76}
{"user_id":"U0003","genre":"Classical","play_count":59}
{"user_id":"U0004","genre":"Folk","play_count":43}
{"user_id":"U0005","genre":"R&B","play_count":84}
{"user_id":"U0006","genre":"Rock","play_count":42}
{"user_id":"U0007","genre":"R&B","play_count":46}
{"user_id":"U0008","genre":"Folk","play_count":49}
{"user_id":"U0009","genre":"R&B","play_count":48}
{"user_id":"U0010","genre":"Electronic","play_count":53}
{"user_id":"U0011","genre":"R&B","play_count":78}
{"user_id":"U0012","genre":"R&B","play_count":83}
{"user_id":"U0013","genre":"Rock","play_count":44}
```

### 2. Calculate the average listen time per song
Computes the average duration (in seconds) for each song based on user play history.

Results saved to: `output/avg_listen_time_per_song/`

Sample output:
```json
{"song_id":"S0421","avg_duration_sec":224.06578947368422,"play_count":76}
{"song_id":"S0065","avg_duration_sec":225.63013698630138,"play_count":73}
{"song_id":"S0386","avg_duration_sec":217.52857142857144,"play_count":70}
{"song_id":"S0293","avg_duration_sec":234.83823529411765,"play_count":68}
{"song_id":"S0014","avg_duration_sec":202.0441176470588,"play_count":68}
{"song_id":"S0267","avg_duration_sec":220.1044776119403,"play_count":67}
```

### 3. List the top 10 most played songs this week
Determines which songs were played the most in the current week and returns the top 10 based on play count.

Results saved to: `output/top_songs_this_week/`

Sample output:
```json
{"song_id":"S0244","title":"Fantastic Moon","artist":"Urban Soul","play_count":23}
{"song_id":"S0011","title":"Wandering Mountain","artist":"The Echoes","play_count":23}
{"song_id":"S0178","title":"Crazy River","artist":"Ocean Waves","play_count":22}
{"song_id":"S0310","title":"Yearning Moon","artist":"The Echoes","play_count":22}
{"song_id":"S0230","title":"Zealous Sun","artist":"Melody Makers","play_count":20}
{"song_id":"S0044","title":"Infinite Dance","artist":"Crystal Sound","play_count":20}
{"song_id":"S0065","title":"Midnight River","artist":"Violet Harmony","play_count":20}
{"song_id":"S0421","title":"Perfect Love","artist":"Folk Fusion","play_count":20}
{"song_id":"S0327","title":"Sweet Sky","artist":"Melody Makers","play_count":19}
```

### 4. Recommend "Happy" songs to users who mostly listen to "Sad" songs
Finds users who primarily listen to "Sad" songs and recommends up to 3 "Happy" songs they haven't played yet.

Results saved to: `output/happy_recommendations/`

Sample output:
```json
{"user_id":"U0001","song_id":"S0486","title":"Beautiful Dance","artist":"Neon Dreams"}
{"user_id":"U0001","song_id":"S0314","title":"Crazy Wave","artist":"Midnight Rhythm"}
{"user_id":"U0001","song_id":"S0442","title":"Undefined Heart","artist":"Folk Fusion"}

{"user_id":"U0024","song_id":"S0044","title":"Infinite Dance","artist":"Crystal Sound"}
{"user_id":"U0024","song_id":"S0482","title":"Yearning Sunrise 481","artist":"Melody Makers"}
{"user_id":"U0024","song_id":"S0416","title":"Radiant Sun","artist":"Sound Collective"}
```

### 5. Compute the genre loyalty score for each user
For each user, calculates the proportion of their plays that belong to their most-listened genre. Outputs users with a loyalty score above 0.8.

Results saved to: `output/genre_loyalty_scores/`

Sample output:
```json
{"user_id":"U0073","genre":"Pop","loyalty_score":1.0}
```

### 6. Identify users who listen to music between 12 AM and 5 AM
Extracts users who frequently listen to music between 12 AM and 5 AM based on their listening timestamps.

Results saved to: `output/night_owl_users/`

Sample output:
```json
{"user_id":"U0037","night_plays":88}
{"user_id":"U0061","night_plays":77}
{"user_id":"U0002","night_plays":73}
{"user_id":"U0044","night_plays":73}
{"user_id":"U0016","night_plays":73}
{"user_id":"U0038","night_plays":72}
```

## Challenges and Solutions

- **Challenge**: Handling timestamp conversions in Spark
  - **Solution**: Used Spark's built-in functions to convert string timestamps to proper timestamp objects for filtering and analysis

- **Challenge**: Creating efficient recommendations system with Spark
  - **Solution**: Used a combination of filtering and anti-joins to identify songs a user hasn't listened to yet

- **Challenge**: Calculating loyalty scores efficiently
  - **Solution**: Used window functions to avoid multiple aggregations and improve performance

## Output Structure

Results are organized in the following folder structure:

```
output/
├── user_favorite_genres/
├── avg_listen_time_per_song/
├── top_songs_this_week/
├── happy_recommendations/
├── genre_loyalty_scores/
├── night_owl_users/
└── enriched_logs/
```
