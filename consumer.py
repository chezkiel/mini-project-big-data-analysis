import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from kafka import KafkaConsumer
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["cuaca_db"]
collection = db["data_cuaca"]

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
TOPIC_NAME = os.getenv("TOPIC_NAME")

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='cuaca-consumer',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("[Consumer aktif] Menunggu data cuaca dari Kafka...\n")

for message in consumer:
    raw_data = message.value

    city = raw_data.get("name", "Unknown")
    main = raw_data.get("main", {})

    # Tangani kasus 'weather' yang berupa string JSON atau list dict
    weather_field = raw_data.get("weather")
    if isinstance(weather_field, str):
        try:
            weather_list = json.loads(weather_field)
        except json.JSONDecodeError:
            weather_list = []
    else:
        weather_list = weather_field if weather_field else []

    weather_desc = weather_list[0].get("description", "Unknown") if weather_list else "Unknown"

    temp = main.get("temp")
    humidity = main.get("humidity")
    pressure = main.get("pressure")
    wind_speed = raw_data.get("wind", {}).get("speed")
    coords = raw_data.get("coord", {})
    timestamp_utc = raw_data.get("dt")
    timezone_offset = raw_data.get("timezone", 0)

    if temp is None or humidity is None or timestamp_utc is None:
        print("Data tidak lengkap, dilewati.")
        continue

    utc_datetime = datetime.utcfromtimestamp(timestamp_utc)
    local_datetime = utc_datetime + timedelta(seconds=timezone_offset)
    local_time_str = local_datetime.strftime("%Y-%m-%d %H:%M:%S")

    print(f"Kota: {city} | Suhu: {temp}°C | Kelembaban: {humidity}% | Cuaca: {weather_desc}")
    print(f"Waktu lokal: {local_time_str}")
    print("-" * 40)

    data_to_save = {
        "city": city,
        "temperature": temp,
        "humidity": humidity,
        "pressure": pressure,
        "weather": weather_desc,
        "wind_speed": wind_speed,
        "coordinates": coords,
        "local_time": local_time_str,
    }

    collection.insert_one(data_to_save)