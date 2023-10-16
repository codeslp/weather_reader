from urllib.parse import urlencode
import os
import httpx
from dotenv import load_dotenv
from config import cities

load_dotenv("/Users/bfaris96/Desktop/turing-proj/weather_reader/.env")


def get_lat_long(cities=cities) -> dict:
    base_url = "http://api.openweathermap.org/geo/1.0/direct?"
    API_KEY = os.getenv("API_KEY")
    
    for city, co_st in cities.items():
        country_code = co_st['country']
        
        if country_code == "US":
            # If the country is US, then include state code
            q_value = f"{city},{co_st['state']},{country_code}"
        else:
            # For other countries, just use city and country code
            q_value = f"{city},{country_code}"
        
        params = {
            "q": q_value,
            "limit": 1,
            "appid": API_KEY
        }
        
        url = base_url + urlencode(params)
        response = httpx.get(url)
        response.raise_for_status()
        data = response.json()
        cities[city]["lat"] = data[0]["lat"]
        cities[city]["lon"] = data[0]["lon"]
        
    return cities

