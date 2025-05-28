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
CITY = os.getenv("CITY")

def get_weather(city):
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": API_KEY,
        "units": "metric"
    }
    response = requests.get(url, params=params)
    return response.json()

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print(f"[Producer aktif] Mengirim data cuaca untuk kota: {CITY}...\n")

    while True:
        data = get_weather(CITY)
        producer.send(TOPIC_NAME, data)
        producer.flush()
        print(f"Data cuaca dikirim: {data['name']} | {data['main']['temp']}°C")
        time.sleep(300) 

if __name__ == "__main__":
    main()