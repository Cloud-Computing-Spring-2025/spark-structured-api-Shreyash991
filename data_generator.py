import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta
import sys

# Create data directory if it doesn't exist
os.makedirs('data', exist_ok=True)

# Generate songs metadata
def generate_songs_metadata(num_songs=1000):
    print("Generating songs metadata...")
    song_ids = [f"S{str(i).zfill(4)}" for i in range(1, num_songs+1)]
    
    # Song title patterns
    adjectives = ["Beautiful", "Crazy", "Dark", "Electric", "Fantastic", "Golden", "Happy", "Infinite", 
                 "Jazzy", "Kind", "Lost", "Midnight", "Neon", "Ocean", "Perfect", "Quiet", "Radiant", 
                 "Sweet", "Timeless", "Undefined", "Vibrant", "Wandering", "Yearning", "Zealous"]
    
    nouns = ["Love", "Heart", "Dream", "Sky", "River", "Mountain", "Star", "Moon", "Sun", "Memory",
            "Journey", "Dance", "Song", "Ocean", "Fire", "Time", "Light", "Shadow", "Sunrise", "Wave"]
    
    artists = ["The Echoes", "Midnight Rhythm", "Stellar Band", "Electric Phoenix", "Crystal Sound",
              "Neon Dreams", "Violet Harmony", "Ocean Waves", "Urban Soul", "Digital Pulse",
              "Acoustic Hearts", "Jazz Ensemble", "Rock Rebels", "Pop Paradise", "Folk Fusion",
              "Classic Revival", "Beat Masters", "Sound Collective", "Rhythm Section", "Melody Makers"]
    
    genres = ["Pop", "Rock", "Jazz", "Classical", "Hip Hop", "R&B", "Electronic", "Country", "Folk", "Indie"]
    
    moods = ["Happy", "Sad", "Energetic", "Chill", "Romantic", "Melancholic", "Nostalgic", "Upbeat", "Dreamy", "Intense"]
    
    # Generate song titles
    titles = []
    used_titles = set()
    
    # Progress tracking
    progress_interval = max(1, num_songs // 10)
    
    for i in range(num_songs):
        # Progress reporting
        if i % progress_interval == 0:
            print(f"Generating song {i}/{num_songs} ({(i/num_songs)*100:.1f}%)...")
            sys.stdout.flush()
            
        # Instead of potentially infinite loop, use a counter
        attempts = 0
        max_attempts = 100
        
        while attempts < max_attempts:
            title = f"{random.choice(adjectives)} {random.choice(nouns)}"
            if title not in used_titles:
                used_titles.add(title)
                titles.append(title)
                break
            attempts += 1
            
        # If we couldn't generate a unique title, add a number suffix
        if attempts >= max_attempts:
            title = f"{random.choice(adjectives)} {random.choice(nouns)} {i}"
            used_titles.add(title)
            titles.append(title)
    
    # Create dataframe
    print("Creating songs dataframe...")
    songs_df = pd.DataFrame({
        'song_id': song_ids,
        'title': titles,
        'artist': [random.choice(artists) for _ in range(num_songs)],
        'genre': [random.choice(genres) for _ in range(num_songs)],
        'mood': [random.choice(moods) for _ in range(num_songs)]
    })
    
    # Save to CSV
    print("Saving songs to CSV...")
    songs_df.to_csv('data/songs_metadata.csv', index=False)
    print(f"Generated {num_songs} songs in data/songs_metadata.csv")
    return songs_df

# Generate listening logs
def generate_listening_logs(songs_df, num_users=100, days=30, avg_plays_per_user_per_day=5):
    print("Generating listening logs...")
    user_ids = [f"U{str(i).zfill(4)}" for i in range(1, num_users+1)]
    song_ids = songs_df['song_id'].tolist()
    
    # Create end date as today and start date as 30 days ago
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Lists to store the data
    log_user_ids = []
    log_song_ids = []
    log_timestamps = []
    log_durations = []
    
    # Progress tracking
    progress_interval = max(1, num_users // 10)
    
    # For each user
    for i, user_id in enumerate(user_ids):
        # Progress reporting
        if i % progress_interval == 0:
            print(f"Generating logs for user {i}/{num_users} ({(i/num_users)*100:.1f}%)...")
            sys.stdout.flush()
            
        # Assign genre and mood preferences to users
        fav_genres = random.sample(songs_df['genre'].unique().tolist(), k=min(3, len(songs_df['genre'].unique())))
        fav_moods = random.sample(songs_df['mood'].unique().tolist(), k=min(3, len(songs_df['mood'].unique())))
        
        # Weighted song selection based on preferences
        song_weights = []
        for _, song in songs_df.iterrows():
            weight = 1.0
            if song['genre'] in fav_genres:
                weight *= 3.0  # Higher weight for favorite genres
            if song['mood'] in fav_moods:
                weight *= 2.0  # Higher weight for favorite moods
            song_weights.append(weight)
        
        # Normalize weights
        total_weight = sum(song_weights)
        if total_weight > 0:
            song_weights = [w/total_weight for w in song_weights]
        else:
            song_weights = [1.0/len(song_weights) for _ in song_weights]
        
        # Generate random number of plays for this user
        num_plays = int(np.random.normal(avg_plays_per_user_per_day * days, avg_plays_per_user_per_day * days / 4))
        num_plays = max(1, num_plays)  # At least 1 play
        
        for _ in range(num_plays):
            # Select song based on preferences
            song_idx = np.random.choice(len(song_ids), p=song_weights)
            song_id = song_ids[song_idx]
            
            # Generate random timestamp within the date range
            random_seconds = random.randint(0, int((end_date - start_date).total_seconds()))
            timestamp = start_date + timedelta(seconds=random_seconds)
            
            # Generate duration (between 30 seconds and 5 minutes)
            # Higher probability of full song play for favorite genres/moods
            if songs_df.iloc[song_idx]['genre'] in fav_genres or songs_df.iloc[song_idx]['mood'] in fav_moods:
                # Full song play for favorites (3-5 minutes)
                duration = random.randint(180, 300)
            else:
                # More variable play time for non-favorites (30 sec - 5 min)
                duration = random.randint(30, 300)
            
            log_user_ids.append(user_id)
            log_song_ids.append(song_id)
            log_timestamps.append(timestamp.strftime('%Y-%m-%d %H:%M:%S'))
            log_durations.append(duration)
    
    print("Creating listening logs dataframe...")
    logs_df = pd.DataFrame({
        'user_id': log_user_ids,
        'song_id': log_song_ids,
        'timestamp': log_timestamps,
        'duration_sec': log_durations
    })
    
    # Save to CSV
    print("Saving listening logs to CSV...")
    logs_df.to_csv('data/listening_logs.csv', index=False)
    print(f"Generated {len(logs_df)} listening records for {num_users} users in data/listening_logs.csv")

if __name__ == "__main__":
    try:
        # Reduce the number of songs to make it faster if necessary
        songs_df = generate_songs_metadata(num_songs=500)
        generate_listening_logs(songs_df, num_users=100, days=30, avg_plays_per_user_per_day=8)
        print("Data generation completed successfully.")
    except Exception as e:
        print(f"Error generating data: {e}")
        import traceback
        traceback.print_exc()
