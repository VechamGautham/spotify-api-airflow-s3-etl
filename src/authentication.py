import json
import requests
from typing import Any, Dict

TOKEN_URL = "https://accounts.spotify.com/api/token"

def get_token(client_id: str, client_secret: str, url: str = TOKEN_URL) -> Dict[Any, Any]:
    """Request a client-credentials access token from Spotify."""
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    try:
        resp = requests.post(url=url, headers=headers, data=payload, timeout=30)
        resp.raise_for_status()
        return json.loads(resp.content)
    except Exception as err:
        print(f"[get_token] Error: {err}")
        return {}

def get_auth_header(access_token: str) -> Dict[str, str]:
    """Return the Authorization header for Spotify Web API calls."""
    return {"Authorization": f"Bearer {access_token}"} 
