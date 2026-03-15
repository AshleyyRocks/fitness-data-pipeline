import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
import requests
from dotenv import load_dotenv

load_dotenv()

STRAVA_CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
STRAVA_ACCESS_TOKEN = os.getenv("STRAVA_ACCESS_TOKEN")
STRAVA_REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")
STRAVA_EXPIRES_AT = os.getenv("STRAVA_EXPIRES_AT")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")

TOKEN_URL = "https://www.strava.com/api/v3/oauth/token"
ACTIVITIES_URL = "https://www.strava.com/api/v3/athlete/activities"


def require_env(name: str, value: str | None) -> str:
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def save_env_value(key: str, value: str) -> None:
    env_path = Path(".env")
    lines = []

    if env_path.exists():
        lines = env_path.read_text().splitlines()

    updated = False
    new_lines = []

    for line in lines:
        if line.startswith(f"{key}="):
            new_lines.append(f"{key}={value}")
            updated = True
        else:
            new_lines.append(line)

    if not updated:
        new_lines.append(f"{key}={value}")

    env_path.write_text("\n".join(new_lines) + "\n")


def refresh_access_token_if_needed() -> str:
    client_id = require_env("STRAVA_CLIENT_ID", STRAVA_CLIENT_ID)
    client_secret = require_env("STRAVA_CLIENT_SECRET", STRAVA_CLIENT_SECRET)
    refresh_token = require_env("STRAVA_REFRESH_TOKEN", STRAVA_REFRESH_TOKEN)
    access_token = require_env("STRAVA_ACCESS_TOKEN", STRAVA_ACCESS_TOKEN)

    now_ts = int(datetime.now(timezone.utc).timestamp())
    expires_at = int(STRAVA_EXPIRES_AT) if STRAVA_EXPIRES_AT else 0

    # If token is still valid for at least 5 more minutes, use it
    if expires_at and now_ts < (expires_at - 300):
        return access_token

    print("Refreshing Strava access token...")

    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }

    response = requests.post(TOKEN_URL, data=payload, timeout=30)
    response.raise_for_status()
    token_data = response.json()

    new_access_token = token_data["access_token"]
    new_refresh_token = token_data["refresh_token"]
    new_expires_at = str(token_data["expires_at"])

    save_env_value("STRAVA_ACCESS_TOKEN", new_access_token)
    save_env_value("STRAVA_REFRESH_TOKEN", new_refresh_token)
    save_env_value("STRAVA_EXPIRES_AT", new_expires_at)

    print("Saved refreshed Strava tokens to .env")

    return new_access_token


def fetch_activities(access_token: str, per_page: int = 50) -> list[dict]:
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    params = {
        "page": 1,
        "per_page": per_page,
    }

    response = requests.get(
        ACTIVITIES_URL,
        headers=headers,
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def save_raw_json_locally(data: list[dict], now: datetime) -> Path:
    folder = Path("data") / "raw" / "strava" / now.strftime("%Y") / now.strftime("%m")
    folder.mkdir(parents=True, exist_ok=True)

    filename = f"activities_{now.strftime('%Y_%m_%dT%H%M%SZ')}.json"
    file_path = folder / filename

    payload = {
        "pulled_at_utc": now.isoformat(),
        "source": "strava",
        "record_count": len(data),
        "activities": data,
    }

    file_path.write_text(json.dumps(payload, indent=2))
    print(f"Saved raw JSON locally: {file_path}")
    return file_path


def flatten_activities(activities: list[dict], pulled_at_utc: str) -> list[dict]:
    flattened = []

    for activity in activities:
        row = {
            "pulled_at_utc": pulled_at_utc,
            "activity_id": activity.get("id"),
            "name": activity.get("name"),
            "type": activity.get("type"),
            "sport_type": activity.get("sport_type"),
            "start_date": activity.get("start_date"),
            "timezone": activity.get("timezone"),
            "utc_offset": activity.get("utc_offset"),
            "distance_meters": activity.get("distance"),
            "moving_time_seconds": activity.get("moving_time"),
            "elapsed_time_seconds": activity.get("elapsed_time"),
            "total_elevation_gain": activity.get("total_elevation_gain"),
            "start_latlng": json.dumps(activity.get("start_latlng")),
            "end_latlng": json.dumps(activity.get("end_latlng")),
            "achievement_count": activity.get("achievement_count"),
            "kudos_count": activity.get("kudos_count"),
            "comment_count": activity.get("comment_count"),
            "athlete_count": activity.get("athlete_count"),
            "photo_count": activity.get("photo_count"),
            "average_speed": activity.get("average_speed"),
            "max_speed": activity.get("max_speed"),
            "average_cadence": activity.get("average_cadence"),
            "average_watts": activity.get("average_watts"),
            "weighted_average_watts": activity.get("weighted_average_watts"),
            "kilojoules": activity.get("kilojoules"),
            "device_watts": activity.get("device_watts"),
            "has_heartrate": activity.get("has_heartrate"),
            "average_heartrate": activity.get("average_heartrate"),
            "max_heartrate": activity.get("max_heartrate"),
            "elev_high": activity.get("elev_high"),
            "elev_low": activity.get("elev_low"),
            "pr_count": activity.get("pr_count"),
            "visibility": activity.get("visibility"),
            "commute": activity.get("commute"),
            "manual": activity.get("manual"),
            "private": activity.get("private"),
            "flagged": activity.get("flagged"),
            "trainer": activity.get("trainer"),
            "gear_id": activity.get("gear_id"),
            "average_temp": activity.get("average_temp"),
            "calories": activity.get("calories"),
            "suffer_score": activity.get("suffer_score"),
            "upload_id": activity.get("upload_id"),
            "external_id": activity.get("external_id"),
        }
        flattened.append(row)

    return flattened


def save_csv_locally(rows: list[dict], now: datetime) -> Path:
    folder = Path("data") / "analytics" / "strava" / now.strftime("%Y") / now.strftime("%m")
    folder.mkdir(parents=True, exist_ok=True)

    filename = f"activities_{now.strftime('%Y_%m_%dT%H%M%SZ')}.csv"
    file_path = folder / filename

    if not rows:
        headers = ["pulled_at_utc", "activity_id", "name", "type", "start_date"]
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
    else:
        headers = list(rows[0].keys())
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)

    print(f"Saved analytics CSV locally: {file_path}")
    return file_path


def upload_to_s3(file_path: Path) -> str:
    bucket = require_env("S3_BUCKET_NAME", S3_BUCKET_NAME)
    s3 = boto3.client("s3", region_name=AWS_REGION)

    key = str(file_path).replace("\\", "/")
    s3.upload_file(str(file_path), bucket, key)
    print(f"Uploaded to s3://{bucket}/{key}")
    return key


def main() -> None:
    now = datetime.now(timezone.utc)
    pulled_at_utc = now.isoformat()

    access_token = refresh_access_token_if_needed()
    activities = fetch_activities(access_token)

    print(f"Fetched {len(activities)} activities from Strava")
    for activity in activities[:5]:
        print(
            f"- {activity.get('name')} | "
            f"{activity.get('type')} | "
            f"{activity.get('distance')} meters"
        )

    raw_json_path = save_raw_json_locally(activities, now)

    flattened_rows = flatten_activities(activities, pulled_at_utc)
    csv_path = save_csv_locally(flattened_rows, now)

    upload_to_s3(raw_json_path)
    upload_to_s3(csv_path)


if __name__ == "__main__":
    main()