import os
import json
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

prev_temp = None
prev_hum = None

print("[Consumer aktif] Menunggu data cuaca dari Kafka...\n")

for message in consumer:
    raw_data = message.value

    city = raw_data.get('name', 'Unknown')
    main = raw_data.get('main', {})
    weather_desc = raw_data.get('weather', [{}])[0].get('description', 'Unknown')
    temp = main.get('temp')
    hum = main.get('humidity')
    timestamp = raw_data.get('dt')

    if temp is None or hum is None:
        print("Data tidak lengkap, dilewati.")
        continue

    print(f"Kota: {city} | Suhu: {temp}°C | Kelembaban: {hum}% | Cuaca: {weather_desc}")
    print(f"Timestamp (epoch): {timestamp}")

    if prev_temp is not None and prev_hum is not None:
        temp_diff = temp - prev_temp
        hum_diff = hum - prev_hum

        arah_suhu = "naik" if temp_diff > 0 else "turun" if temp_diff < 0 else "stabil"
        arah_hum = "naik" if hum_diff > 0 else "turun" if hum_diff < 0 else "stabil"

        print(f"Perubahan suhu: {temp_diff:+.2f}°C ({arah_suhu})")
        print(f"Perubahan kelembaban: {hum_diff:+.2f}% ({arah_hum})")

    print("-" * 40)

    data_to_save = {
        "city": city,
        "temperature": temp,
        "humidity": hum,
        "weather": weather_desc,
        "timestamp": timestamp
    }
    collection.insert_one(data_to_save)

    prev_temp = temp
    prev_hum = hum