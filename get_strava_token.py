import requests

client_id = "209621"
client_secret = "6fafd674591c49a61da6aae14091a12bdafeec44"
code = "692cdd05b119f0452c8b09eb0e9e7bd92a107785"

url = "https://www.strava.com/api/v3/oauth/token"

payload = {
    "client_id": client_id,
    "client_secret": client_secret,
    "code": code,
    "grant_type": "authorization_code"
}

response = requests.post(url, data=payload)

print(response.status_code)
print(response.json())




