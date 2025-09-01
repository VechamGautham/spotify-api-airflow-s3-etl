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
    
    