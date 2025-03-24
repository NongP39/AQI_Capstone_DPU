import requests

API_KEY = "7b2991a3-cccc-4881-9ff7-2f9d9cf8b753"  # แทนที่ YOUR_API_KEY ด้วย API Key ของคุณ

payload = {
    "city": "Bangkok",
    "state" : "Bangkok",
    "country" : "Thailand",
    "key" : API_KEY
}

url = "http://api.airvisual.com/v2/city?"
response = requests.get(url, params=payload)

print(response.url)
data = response.json()
print(data)