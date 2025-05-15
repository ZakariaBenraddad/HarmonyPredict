import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up Spotify API credentials
client_id = os.getenv('SPOTIFY_CLIENT_ID')
client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')

# Initialize Spotify client
client_credentials_manager = SpotifyClientCredentials(
    client_id=client_id, 
    client_secret=client_secret
)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Test the connection
results = sp.search(q='genre:metal', type='track', limit=10)
tracks = results['tracks']['items']
print(f"Successfully connected to Spotify API! Found {len(tracks)} tracks")
for track in tracks:
    print(f"- {track['name']} by {track['artists'][0]['name']}")
