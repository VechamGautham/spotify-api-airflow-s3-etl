import os
import json 
from dotenv import load_dotenv
from authentication import get_token
from endpoint import get_paginated_album_tracks, get_paginated_new_releases

#Constants 
load_dotenv(".env",override=True)
CLIENT_ID = os.getenv("CLIENT_ID", "")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "")
URL_NEW_RELEASES = "https://api.spotify.com/v1/browse/new-releases"
URL_ALBUMS = "https://api.spotify.com/v1/albums"
COUNTRY = "IN" # India Market 


def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        print("Please set CLIENT_ID and CLIENT_SECRET in your .env file.")
        return
    
    token = get_token(CLIENT_ID,CLIENT_SECRET)
    access_token = token.get("access_token")

    if not access_token:
        print("could not fetch access token.")
        return 
    
    #--- Albums (bronze) ---
    albums = get_paginated_new_releases(base_url=URL_NEW_RELEASES,access_token=access_token,limit=20,country=COUNTRY)
    print(f"fetched {len(albums)} alubms")

    with open("bronze_albums.json", "w", encoding="utf-8") as f :
        json.dump(albums,f,ensure_ascii=False,indent=2)

    #--- Tracks (bronze) --- 
    album_ids = [a.get("id") for a in albums if a.get("id")]
    all_tracks = []

    for idx,album_id in enumerate(album_ids,start =1):
        tracks = get_paginated_album_tracks(base_url=URL_ALBUMS,access_token=access_token,album_id=album_id,limit=50)
        all_tracks.extend(tracks)
        print(f"   [{idx}/{len(album_ids)}] album {album_id} → {len(tracks)} tracks")

    print(f"✅ Fetched {len(all_tracks)} tracks in total.")
    with open("bronze_tracks.json", "w", encoding="utf-8") as f:
        json.dump(all_tracks, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()

