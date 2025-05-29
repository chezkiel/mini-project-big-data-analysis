import os
import json
import time
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

API_KEY = os.getenv("OWM_API_KEY")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
TOPIC_NAME = os.getenv("TOPIC_NAME")
CITY_LIST = os.getenv("CITY_LIST").split(',')

def get_weather(city):
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": API_KEY,
        "units": "metric"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print(f"[Producer active] Sending raw weather data for cities...\n")

    while True:
        for city in CITY_LIST:
            city = city.strip()
            try:
                data = get_weather(city)
                # Kirim data mentah apa adanya, tanpa diubah
                producer.send(TOPIC_NAME, data)
                producer.flush()
                print(f"Sent raw weather data: {city}")
            except Exception as e:
                print(f"[Failed] City: {city} | Error: {e}")

            time.sleep(2)  # delay kecil antar request

        print("✅ One cycle completed. Waiting for 5 minutes...\n")
        time.sleep(300)  # delay 5 menit antar cycle

if __name__ == "__main__":
    main()