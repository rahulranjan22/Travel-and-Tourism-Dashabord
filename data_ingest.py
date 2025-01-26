import requests
from elasticsearch import Elasticsearch
import schedule
import time
import datetime
import os

# Elasticsearch configuration
ES_HOST = os.getenv("ES_HOST", "")
ES_USERNAME = os.getenv("ES_USERNAME", "elastic")
ES_PASSWORD = os.getenv("ES_PASSWORD", "")
WEATHER_INDEX = "weather-data-dashboard"
FLIGHT_INDEX = "flights-data"

# API Configurations
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY", "")
AVIATIONSTACK_API_KEY = os.getenv("AVIATIONSTACK_API_KEY", "")
CITIES = ["London", "Bangalore", "New York"]

# Connect to Elasticsearch
es = Elasticsearch(
    ES_HOST,
    basic_auth=(ES_USERNAME, ES_PASSWORD)
)

# Ensure indices exist
if not es.indices.exists(index=WEATHER_INDEX):
    es.indices.create(index=WEATHER_INDEX)
if not es.indices.exists(index=FLIGHT_INDEX):
    es.indices.create(index=FLIGHT_INDEX)

# Fetch Weather Data
def fetch_weather_data():
    for city in CITIES:
        try:
            url = f"http://api.weatherapi.com/v1/current.json?key={WEATHER_API_KEY}&q={city}&aqi=no"
            response = requests.get(url)
            data = response.json()
            weather_doc = {
                "city": city,
                "temperature": data["current"]["temp_c"],
                "humidity": data["current"]["humidity"],
                "condition": data["current"]["condition"]["text"],
                "timestamp": datetime.datetime.utcnow()
            }
            es.index(index=WEATHER_INDEX, document=weather_doc)
        except Exception as e:
            print(f"Error fetching weather data for {city}: {e}")

# Fetch Flight Data
def fetch_flight_data():
    url = f"http://api.aviationstack.com/v1/flights?access_key={AVIATIONSTACK_API_KEY}"
    try:
        response = requests.get(url)
        data = response.json()
        flight_doc = {
            "flights": data.get("data", []),
            "timestamp": datetime.datetime.utcnow()
        }
        es.index(index=FLIGHT_INDEX, document=flight_doc)
    except Exception as e:
        print(f"Error fetching flight data: {e}")

schedule.every(10).minutes.do(fetch_weather_data)
schedule.every(60).minutes.do(fetch_flight_data)

if __name__ == "__main__":
    fetch_weather_data()
    fetch_flight_data()
    while True:
        schedule.run_pending()
        time.sleep(1)