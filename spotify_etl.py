import os 
from typing import Dict , Any , Callable
from urllib import response
from dotenv import load_dotenv
import json 
from httpx import get
import requests  

load_dotenv(".env",override=True)


CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# function for getting token via client_id and client_secret which expires even 60 min
def get_token(client_id: str , client_secret: str, url: str) -> Dict[Any,Any]:

    headers = {
        "Content-Type" : "application/x-www-form-urlencoded"
    }

    payload = {
        "grant_type" : "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }

    try :
        response = requests.post(url=url,headers=headers,data=payload)
        print(type(response))
        response.raise_for_status()
        response_json = json.loads(response.content)

        return response_json
    
    except Exception as err :
        print(f"Error:{err}")
        return {}
    
URL_TOKEN="https://accounts.spotify.com/api/token"

token = get_token(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, url=URL_TOKEN) # type: ignore


# function for authenticating via access token 

def get_auth_header(access_token:str) -> Dict[str,str]:
    return {"Authorization": f"Bearer {access_token}"} 


def get_new_releases(url:str , access_token: str, offset: int=0, limit: int=20, next: str="") -> Dict[Any,Any]:

    if next == "":
        request_url = f"{url}?offset={offset}&limit={limit}"
    else :
        request_url = f"{next}"
    

    headers = get_auth_header(access_token=access_token)

    try:
        response = requests.get(url=request_url,headers=headers)

        return response.json()
    
    except Exception as err:
        print(f"Error reqeusting data {err}")
        return {"error":err}
    
URL_NEW_RELEASES = "https://api.spotify.com/v1/browse/new-releases"

# Note: the `access_token` value from the dictionary `token` can be retrieved either using `get()` method or dictionary syntax `token['access_token']`
releases_response = get_new_releases(url=URL_NEW_RELEASES, access_token=token["access_token"])

print(releases_response)

for n in range(releases_response["albums"]["total"]):
    releases_response = get_new_releases(url=URL_NEW_RELEASES, access_token=token["access_token"],next=releases_response["albums"]["next"])
    print(releases_response)

