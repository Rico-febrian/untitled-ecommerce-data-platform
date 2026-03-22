import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv

# Load .env dari root project
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

# Konfigurasi Redpanda/Kafka — bootstrap.servers = alamat broker
KAFKA_CONFIG = {
    "bootstrap.servers": f"localhost:{os.getenv('REDPANDA_BROKER_EXTERNAL_PORT')}",
}

# Nama topic tempat clickstream events dikirim
TOPIC = "clickstream-events"

# Event types yang mungkin terjadi di e-commerce
# Urutan ini nge-reflect typical user journey: liat → cari → klik → masukin keranjang → checkout
EVENT_TYPES = ["page_view", "search", "product_click", "add_to_cart", "checkout"]

# Probabilitas tiap event type (page_view paling sering, checkout paling jarang — realistis)
EVENT_WEIGHTS = [40, 20, 20, 15, 5]

DEVICES = ["mobile", "desktop", "tablet"]


def create_event(user_ids: list[int], product_ids: list[int]) -> dict:
    """Generate satu fake clickstream event."""
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(user_ids),
        "event_type": random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0],
        "product_id": random.choice(product_ids),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device": random.choice(DEVICES),
        "session_id": str(uuid.uuid4()),
    }


def delivery_callback(err, msg):
    """Callback yang dipanggil setelah message dikirim (sukses atau gagal)."""
    if err:
        print(f"GAGAL kirim: {err}")
    else:
        # msg.topic() = topic tujuan, msg.partition() = partition mana, msg.offset() = posisi di partition
        print(f"Terkirim ke {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}")


def get_ids_from_db():
    """Ambil user_ids dan product_ids dari PostgreSQL biar event realistis (user & product beneran ada)."""
    import psycopg2

    conn = psycopg2.connect(
        host="localhost",
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    cur = conn.cursor()

    cur.execute("SELECT id FROM users")
    user_ids = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT id FROM products")
    product_ids = [r[0] for r in cur.fetchall()]

    conn.close()
    return user_ids, product_ids


def main():
    # Inisialisasi producer dengan config Redpanda
    producer = Producer(KAFKA_CONFIG)

    # Ambil user & product IDs dari database
    user_ids, product_ids = get_ids_from_db()
    print(f"Loaded {len(user_ids)} users, {len(product_ids)} products from DB.")
    print(f"Producing events to topic '{TOPIC}'... (Ctrl+C to stop)\n")

    try:
        while True:
            # Generate satu event
            event = create_event(user_ids, product_ids)

            # Kirim ke Redpanda
            # - topic: kemana
            # - value: isi message (harus bytes, makanya di-encode)
            # - callback: fungsi yang dipanggil setelah kirim
            producer.produce(
                topic=TOPIC,
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_callback,
            )

            # poll() trigger callback buat messages yang udah selesai dikirim
            # Tanpa ini, callback nggak akan kepanggil
            producer.poll(0)

            # Jeda random 0.5-2 detik biar simulate traffic realistis
            time.sleep(random.uniform(0.5, 2.0))

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        # flush() = pastiin semua message yang masih di buffer terkirim sebelum exit
        producer.flush()
        print("Producer stopped. All buffered messages flushed.")


if __name__ == "__main__":
    main()
