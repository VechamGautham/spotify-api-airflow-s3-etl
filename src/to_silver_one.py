from encodings.punycode import T
import os 
import json 
import csv 
from datetime import date 

# ---Paths ---

BRONZE_DIR = "bronze"
SILVER_DIR = os.path.join("silver","tracks")
os.makedirs(SILVER_DIR, exist_ok=True)

BRONZE_TRACKS_FULL = os.path.join(BRONZE_DIR, "bronze_tracks_full.json")
BRONZE_ALBUMS      = os.path.join(BRONZE_DIR, "bronze_albums.json")
BRONZE_ARTISTS     = os.path.join(BRONZE_DIR, "bronze_artists.json")

OUTPUT_CSV         = os.path.join(SILVER_DIR, "tracks_silver.csv")

def ensure_dirs():
    """Make sure the Silver output folder exists."""
    os.makedirs(SILVER_DIR,exist_ok=True)

def load_json(path: str):
    """Small helper to red a JSON file into python."""
    with open(path,"r",encoding="utf-8") as f:
        return json.load(f)

def build_lookups(albums: list,artists: list):
    """
    Turn the Bronze lists into fast lookup dicts:
        albums_by_id[album_id] -> album object
        artists_by_id[artist_id] -> artist object
    """

    # Keep only items that actually have an "id"
    albums_by_id = {a.get("id"): a for a in albums if a and a.get("id")}
    artists_by_id = {ar.get("id"): ar for ar in artists if ar and ar.get("id")}

    return albums_by_id,artists_by_id

def make_rows_first_seen(tracks_full: list,
                         albums_by_id: dict,
                         artists_by_id: dict,
                         ingestion_date: str) -> list:
    """
    Build denormalized rows (one per unique track_id).
    Dedup rule: if we've already seen a track_id, skip it (first-seen wins).
    """
    rows = []
    seen_track_ids = set()

    for t in tracks_full:
        track_id = t.get("id")
        if not track_id:
            continue

        # --- simple dedup: first seen wins ---
        if track_id in seen_track_ids:
            continue
        seen_track_ids.add(track_id)

        # Track basics
        track_name  = t.get("name", "")
        duration_ms = t.get("duration_ms")
        explicit    = t.get("explicit")
        popularity  = t.get("popularity")
        markets_cnt = len(t.get("available_markets") or [])

        # Album (nested first; enrich from albums_by_id if missing bits)
        album_obj = t.get("album") or {}
        album_id  = album_obj.get("id")
        album_name = album_obj.get("name", "")
        album_type = album_obj.get("album_type", "")
        release_date = album_obj.get("release_date", "")
        release_date_precision = album_obj.get("release_date_precision", "")

        if album_id and (not album_name or not album_type or not release_date or not release_date_precision):
            alb = albums_by_id.get(album_id, {})
            album_name = album_name or alb.get("name", "")
            album_type = album_type or alb.get("album_type", "")
            release_date = release_date or alb.get("release_date", "")
            release_date_precision = release_date_precision or alb.get("release_date_precision", "")

        # Primary artist (first artist in list), enrich via artists_by_id
        artists_list = t.get("artists", []) or album_obj.get("artists", [])
        primary_artist_id = ""
        primary_artist_name = ""
        primary_artist_popularity = None
        primary_artist_genres = ""

        if artists_list:
            primary = artists_list[0]
            primary_artist_id = primary.get("id", "") or ""
            primary_artist_name = primary.get("name", "") or ""
            a_full = artists_by_id.get(primary_artist_id, {})
            primary_artist_popularity = a_full.get("popularity") or None
            genres = a_full.get("genres", [])
            if genres:
                primary_artist_genres = "|".join(str(g) for g in genres if g)

        row = {
            "track_id": track_id,
            "track_name": track_name,
            "album_id": album_id or "",
            "album_name": album_name,
            "album_type": album_type,
            "release_date": release_date,
            "release_date_precision": release_date_precision,
            "duration_ms": duration_ms,
            "explicit": explicit,
            "track_popularity": popularity,
            "primary_artist_id": primary_artist_id,
            "primary_artist_name": primary_artist_name,
            "primary_artist_popularity": primary_artist_popularity,
            "primary_artist_genres": primary_artist_genres,
            "available_markets_count": markets_cnt,
            "ingestion_date": ingestion_date,
        }

        rows.append(row)

    return rows

def write_csv(rows: list, path: str):
    """Write the denormalized Silver rows to a Csv with a fixed column order."""

    if not rows:
        print("No rows to write")
        return 


    fieldnames = [
        "track_id",
        "track_name",
        "album_id",
        "album_name",
        "album_type",
        "release_date",
        "release_date_precision",
        "duration_ms",
        "explicit",
        "track_popularity",
        "primary_artist_id",
        "primary_artist_name",
        "primary_artist_popularity",
        "primary_artist_genres",
        "available_markets_count",
        "ingestion_date",
    ]

    # ensure output folder exists
    os.makedirs(SILVER_DIR, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)



def main():
    # ensure output folder exists 
    ensure_dirs()

    # load bronze JSONs
    tracks_full = load_json(BRONZE_TRACKS_FULL)
    albums = load_json(BRONZE_ALBUMS)
    artists = load_json(BRONZE_ARTISTS)

    # quick sanity log 
    print(f"loaded bronze: tracks_full={len(tracks_full)}, albums={len(albums)}, artists={len(artists)}")

    # dict for fast lookup 
    albums_by_id , artists_by_id = build_lookups(albums=albums,artists=artists)
    print(f"albums_by_id={len(albums_by_id)}, artists_by_id={len(artists_by_id)}")

    ingestion_date = str(date.today())

    rows = make_rows_first_seen(tracks_full=tracks_full,albums_by_id=albums_by_id,artists_by_id=artists_by_id,ingestion_date=ingestion_date)

    print(f"prepared {len(rows)} unique track rows")

    write_csv(rows,OUTPUT_CSV)

    print(f"Wrote{len(rows)} rows to {OUTPUT_CSV}")

    

if __name__ == "__main__":
    main()
