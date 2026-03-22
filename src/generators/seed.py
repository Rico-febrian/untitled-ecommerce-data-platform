import os
import random
from datetime import timedelta
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from faker import Faker

# Load environment variables dari file .env di root project
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

# Inisialisasi Faker dengan locale Indonesia biar nama & kota realistis
fake = Faker("id_ID")

# Konfigurasi koneksi PostgreSQL, semua dari .env
DB_CONFIG = {
    "host": "localhost",
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

# Konstanta buat generate data
CATEGORIES = ["Electronics", "Fashion", "Home & Living", "Sports", "Books", "Food & Beverage"]
PAYMENT_METHODS = ["bank_transfer", "credit_card", "e_wallet", "cod"]
ORDER_STATUSES = ["pending", "paid", "shipped", "delivered", "cancelled"]

NUM_USERS = 200
NUM_PRODUCTS = 50
NUM_ORDERS = 500


def get_connection():
    """Buka koneksi ke PostgreSQL."""
    return psycopg2.connect(**DB_CONFIG)


def run_schema(conn):
    """Baca schema.sql dan execute buat create semua tabel."""
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path) as f:
        conn.cursor().execute(f.read())
    conn.commit()
    print("Schema created.")


def seed_users(conn):
    """Generate fake users dan insert ke tabel users."""
    cur = conn.cursor()
    for _ in range(NUM_USERS):
        cur.execute(
            "INSERT INTO users (name, email, city, registered_at) VALUES (%s, %s, %s, %s)",
            (
                fake.name(),
                fake.unique.email(),  # unique biar nggak ada email duplikat
                fake.city(),
                fake.date_time_between(start_date="-1y"),  # registered dalam 1 tahun terakhir
            ),
        )
    conn.commit()
    print(f"Seeded {NUM_USERS} users.")


def seed_products(conn):
    """Generate fake products dan insert ke tabel products."""
    cur = conn.cursor()
    for _ in range(NUM_PRODUCTS):
        cur.execute(
            "INSERT INTO products (name, category, price, stock) VALUES (%s, %s, %s, %s)",
            (
                fake.catch_phrase(),  # generate nama produk random
                random.choice(CATEGORIES),
                round(random.uniform(10000, 5000000), 2),  # harga antara 10rb - 5jt
                random.randint(10, 500),
            ),
        )
    conn.commit()
    print(f"Seeded {NUM_PRODUCTS} products.")


def seed_orders(conn):
    """Generate fake orders beserta order_items dan payments."""
    cur = conn.cursor()

    # Ambil semua user_id yang udah di-seed
    cur.execute("SELECT id FROM users")
    user_ids = [r[0] for r in cur.fetchall()]

    # Ambil semua product (id + price) buat bikin order items
    cur.execute("SELECT id, price FROM products")
    products = cur.fetchall()

    for _ in range(NUM_ORDERS):
        # Pilih random user dan status
        user_id = random.choice(user_ids)
        order_date = fake.date_time_between(start_date="-6m")  # order dalam 6 bulan terakhir
        status = random.choice(ORDER_STATUSES)

        # Insert order dulu dengan total 0 (di-update nanti setelah items di-insert)
        cur.execute(
            "INSERT INTO orders (user_id, order_date, status, total_amount) VALUES (%s, %s, %s, 0) RETURNING id",
            (user_id, order_date, status),
        )
        order_id = cur.fetchone()[0]

        # Insert 1-5 random order items
        total = 0
        for product_id, product_price in random.sample(products, k=random.randint(1, 5)):
            qty = random.randint(1, 3)
            line_total = float(product_price) * qty
            total += line_total
            cur.execute(
                "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (%s, %s, %s, %s)",
                (order_id, product_id, qty, product_price),
            )

        # Update total_amount di order berdasarkan jumlah semua items
        cur.execute("UPDATE orders SET total_amount = %s WHERE id = %s", (round(total, 2), order_id))

        # Insert payment kalo order bukan pending (pending = belum bayar)
        if status != "pending":
            payment_status = "failed" if status == "cancelled" else "success"
            # Kalo sukses, paid_at = order_date + random 1-60 menit
            paid_at = order_date + timedelta(minutes=random.randint(1, 60)) if payment_status == "success" else None
            cur.execute(
                "INSERT INTO payments (order_id, payment_method, amount, status, paid_at) VALUES (%s, %s, %s, %s, %s)",
                (order_id, random.choice(PAYMENT_METHODS), round(total, 2), payment_status, paid_at),
            )

    conn.commit()
    print(f"Seeded {NUM_ORDERS} orders with items and payments.")


def main():
    conn = get_connection()
    try:
        run_schema(conn)
        seed_users(conn)
        seed_products(conn)
        seed_orders(conn)
        print("Seed complete!")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
