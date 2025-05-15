import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import os
from dotenv import load_dotenv
import time
import spotify_auth
# Load environment variables
load_dotenv()



# Set target country (ISO code)
TARGET_COUNTRY = 'US'  # United Kingdom
def get_featured_playlists(country_code):
    """Get featured playlists for a specific country"""
    try:
        playlists = []
        results = spotify_auth.sp.featured_playlists(country=country_code, limit=50)
        
        # Extract all playlists
        playlists.extend(results['playlists']['items'])
        
        # Handle pagination
        while results['playlists']['next'] and len(playlists) < 100:
            results = spotify_auth.sp.next(results['playlists'])
            playlists.extend(results['playlists']['items'])
            
        return playlists
    except spotipy.exceptions.SpotifyException as e:
        print(f"Featured playlists not available for {country_code}: {str(e)}")
        return []

def get_country_top_playlists(country_code):
    """Get country-specific top charts"""
    try:
        # Try different search queries to get more playlists
        queries = [
            f"top {country_code}",
            f"charts {country_code}",
            f"hits {country_code}",
            f"popular {country_code}"
        ]
        
        all_playlists = []
        for query in queries:
            try:
                results = spotify_auth.sp.search(query, type='playlist', limit=10)
                if results and 'playlists' in results and 'items' in results['playlists']:
                    all_playlists.extend(results['playlists']['items'])
                time.slelep(1)  # Respect rate limits
            except Exception as e:
                print(f"Error with query '{query}': {str(e)}")
                continue
            
        if not all_playlists:
            print(f"No playlists found for country code: {country_code}")
            return []
            
        # Remove duplicates based on playlist ID
        unique_playlists = {p['id']: p for p in all_playlists}.values()
        return list(unique_playlists)
    except Exception as e:
        print(f"Error getting top playlists: {str(e)}")
        return []

def get_tracks_from_playlist(playlist_id):
    """Extract tracks from a playlist"""
    results = spotify_auth.sp.playlist_items(playlist_id, limit=100)
    tracks = results['items']
    
    # Handle pagination
    while results['next'] and len(tracks) < 200:
        results = spotify_auth.sp.next(results)
        tracks.extend(results['items'])
        
    return tracks

def extract_track_data(tracks):
    """Extract relevant track data"""
    track_data = []
    
    for item in tracks:
        if item['track'] is None:
            continue
            
        track = item['track']
        
        # Basic track info
        track_info = {
            'id': track['id'],
            'name': track['name'],
            'artist': track['artists'][0]['name'],
            'artist_id': track['artists'][0]['id'],
            'album': track['album']['name'],
            'popularity': track['popularity'],
            'duration_ms': track['duration_ms'],
            'explicit': track['explicit']
        }
        
        track_data.append(track_info)
    
    return track_data

def get_audio_features_batch(track_ids):
    """Get audio features for a batch of tracks"""
    # Spotify API allows max 100 tracks per request
    feature_data = []
    
    # Process in batches of 100
    for i in range(0, len(track_ids), 100):
        batch = track_ids[i:i+100]
        features = spotify_auth.sp.audio_features(batch)
        feature_data.extend(features)
        # Respect API rate limits
        time.sleep(1)
        
    return feature_data

# Main data collection process
def collect_country_music_data(country_code):
    print(f"Collecting music data for {country_code}...")
    
    # Get featured playlists for the country
    featured_playlists = get_featured_playlists(country_code)
    print(f"Found {len(featured_playlists)} featured playlists")
    
    # Get top chart playlists
    top_playlists = get_country_top_playlists(country_code)
    print(f"Found {len(top_playlists)} chart playlists")
    
    all_playlists = featured_playlists + top_playlists
    
    if not all_playlists:
        print("No playlists found. Please try a different country code.")
        return None
        
    # Extract tracks from playlists
    all_tracks = []
    
    for playlist in all_playlists[:20]:  # Limit to 20 playlists for demonstration
        print(f"Processing playlist: {playlist['name']}")
        tracks = get_tracks_from_playlist(playlist['id'])
        all_tracks.extend(extract_track_data(tracks))
        # Respect API rate limits
        time.sleep(1)
    
    if not all_tracks:
        print("No tracks found in the playlists.")
        return None
        
    # Remove duplicates by track ID
    unique_tracks = {track['id']: track for track in all_tracks if track['id'] is not None}
    tracks_df = pd.DataFrame(list(unique_tracks.values()))
    
    # Get audio features for all tracks
    print(f"Getting audio features for {len(unique_tracks)} tracks")
    track_ids = list(unique_tracks.keys())
    audio_features = get_audio_features_batch(track_ids)
    
    # Create audio features dataframe
    features_df = pd.DataFrame([f for f in audio_features if f is not None])
    
    # Merge track info with audio features
    if not features_df.empty and not tracks_df.empty:
        merged_df = pd.merge(tracks_df, features_df, left_on='id', right_on='id')
        print(f"Successfully collected data for {len(merged_df)} tracks")
        
        # Save to CSV
        output_file = f"spotify_{country_code}_tracks.csv"
        merged_df.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")
        
        return merged_df
    else:
        print("Error: No data collected")
        return None

# Run the data collection
if __name__ == "__main__":
    country_data = collect_country_music_data(TARGET_COUNTRY)
    print("Data collection complete!")

