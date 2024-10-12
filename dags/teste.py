import requests

url = "https://api.airbyte.com/v1/applications/token"

payload = {
    "client_id": "53238c42-23e5-4857-98d6-47ed0b37ffea",
    "client_secret": "ZdoJqcq1EnZjh88ZKcgCVVsqzZWUGVYG"
}
headers = {
    "accept": "application/json",
    "content-type": "application/json"
}

response = requests.post(url, json=payload, headers=headers)

print(response.text)