import requests 
from authentication import get_auth_header 


def get_paginated_new_releases(
        base_url: str,
        access_token: str, 
        limit: int=20, 
        country: str = "IN"
) -> list:
    headers = get_auth_header(access_token)
    request_url = f"{base_url}?limit={limit}&country={country}"
    albums = []

    while request_url:
        response = requests.get(request_url,headers=headers)
        response.raise_for_status()
        data = response.json()

        albums.extend(data["albums"]["items"])
        request_url = data["albums"]["next"] 
    return albums 




def get_paginated_album_tracks(
        base_url: str,
        access_token: str,
        album_id: str,
        limit: int = 50,
) -> list:
    """
    Fetch all tracks for a given album (handles pagination),

    base_url: "https://api.spotify.com/v1/albums"
    """

    headers = get_auth_header(access_token)
    url = f"{base_url}/{album_id}/tracks?limit={limit}"

    tracks = []

    while url:
        response = requests.get(url,headers=headers,timeout=30)
        response.raise_for_status() 
        data = response.json()

        items = data.get("items",[])
        # attach album_id for easier joining later 
        for t in items:
            t["album_id"] = album_id 
        tracks.extend(items)

        url = data.get("next")
    return tracks 



    
def get_tracks_batch(
        base_url: str,
        access_token: str,
        track_ids: list[str],
        chunk_size: int=50
) -> list:
    """
    Fetch full track metadata for a list of track IDs in batches.
    base_url: "https://api.spotify.com/v1/tracks"
    """
    headers = get_auth_header(access_token=access_token)

    #dedup & basic sanity 
    seen = set()
    unique_ids = []
    for tid in track_ids:
        if tid and isinstance(tid,str) and len(tid) == 22 and tid not in seen:
            seen.add(tid)
            unique_ids.append(tid)
    
    full_tracks = []

    for i in range(0,len(unique_ids),chunk_size):
        chunk = unique_ids[i:i+chunk_size]
        url = f"{base_url}?ids={','.join(chunk)}"
        response = requests.get(url,headers=headers,timeout=30)
        response.raise_for_status()
        data = response.json()
        for t in data.get("tracks",[]):
            if t :
                full_tracks.append(t)
    
    return full_tracks




def get_artist_batch(
        base_url: str,
        access_token: str,
        artist_ids: list[str],
        chunk_size: int=50
) -> list:
    """
    Fetch artist metadata for a list of artist IDs in batches.
    base_url: "https://api.spotify.com/v1/artists"
    """

    headers = get_auth_header(access_token=access_token)

    #dedupe 
    seen = set()
    unique_ids = []
    for aid in artist_ids:
        if aid and isinstance(aid,str) and aid not in seen :
            seen.add(aid)
            unique_ids.append(aid)
    
    full_artists = []

    for i in range(0,len(unique_ids),chunk_size):
        chunk = unique_ids[i:i+chunk_size]
        url = f"{base_url}?ids={','.join(chunk)}"

        response = requests.get(url,headers=headers,timeout=30)
        response.raise_for_status()
        data = response.json()
        for a in data.get("artists",[]):
            if a:
                full_artists.append(a)
    
    return full_artists



