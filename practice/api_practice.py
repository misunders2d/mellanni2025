import requests


kiev_coords = (50.4501, 30.5234)
base_url = "https://apis.scrimba.com/openweathermap/data/2.5/"
endpoint = "weather"
params = {
    "lat": kiev_coords[0],
    "lon": kiev_coords[1],
}

res = requests.get(base_url + endpoint, params=params)
print(res.status_code)
print(res.json())