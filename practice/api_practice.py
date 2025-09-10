import requests

urls = {
    "https://api.coingecko.com/api/v3/": "coins/dogecoin",
    "https://apis.scrimba.com/openweathermap/data/2.5/": "weather",
    "https://apis.scrimba.com/unsplash/photos/": "random?orientation=landscape&query=nature",
}

kiev_coords = (50.4501, 30.5234)
base_url = "https://apis.scrimba.com/openweathermap/data/2.5/"
endpoint = "weather"
query_params = {"lat": kiev_coords[0], "lon": kiev_coords[1], "units": "metric"}

# res = requests.get("https://api.coingecko.com/api/v3/coins/bitcoin")
res = requests.get(
    "https://apis.scrimba.com/openweathermap/data/2.5/weather?lat=50.4501&lon=30.5234&units=metric"
)
res2 = requests.get(
    "https://apis.scrimba.com/openweathermap/data/2.5/weather", params=query_params
)
# res = requests.get(base_url + endpoint, params=params)
# res = requests.get("https://api.coingecko.com/api/v3/coins/dogecoin")
# res = requests.get("https://www.google.com/search?q=bed+sheets")
# print(res.status_code)
# print(res.ok)
# print(res.text[:1000])
weather_data = res.json()
weather_data2 = res2.json()
current_weather = weather_data["weather"][0]["description"]
current_weather2 = weather_data2["weather"][0]["description"]
print(current_weather, current_weather2)
# picture_res = requests.get(f"https://apis.scrimba.com/unsplash/photos/random?orientation=landscape&query={current_weather.replace(' ', '+')}")
# print(picture_res.status_code)
# print(picture_res.text)
# print("Current: ", market_data.get('current_price').get('usd'))
# print("24h High: ", market_data.get('high_24h').get('usd'))
# print("24h Low: ", market_data.get('low_24h').get('usd'))

# print(market_data['current_price']['usd'], market_data['current_price']['uah'])

# print(res.json().get('market_data'))
# print(type(res.text))
# print(type(res.json()))
# print(res.content)
