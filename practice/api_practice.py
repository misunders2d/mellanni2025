import requests

urls = {
    "https://api.coingecko.com/api/v3/":"coins/dogecoin",
    "https://apis.scrimba.com/openweathermap/data/2.5/":"weather"}

kiev_coords = (50.4501, 30.5234)
base_url = "https://apis.scrimba.com/openweathermap/data/2.5/"
endpoint = "weather"
params = {
    "lat": kiev_coords[0],
    "lon": kiev_coords[1],
}

# res = requests.get(base_url + endpoint, params=params)
# res = requests.get("https://api.coingecko.com/api/v3/coins/dogecoin")
res = requests.get("https://www.google.com/search?q=bed+sheets")
print(res.status_code)
print(res.content)