import csv
import io
import os
import random
from datetime import datetime, timedelta, timezone

import boto3

PRODUCTS = [
    ("P001", "Молоко 2.5% 1л", "молочные", "Простоквашино", 89.90),
    ("P002", "Кефир 1% 1л", "молочные", "Домик в деревне", 95.50),
    ("P003", "Сметана 20% 400г", "молочные", "Простоквашино", 129.00),
    ("P004", "Творог 5% 300г", "молочные", "Домик в деревне", 149.90),
    ("P005", "Сыр Гауда 200г", "молочные", "Ламбер", 219.00),
    ("P006", "Батон нарезной", "бакалея", "Хлебный дом", 42.00),
    ("P007", "Спагетти 450г", "бакалея", "Barilla", 119.00),
    ("P008", "Рис круглозерный 900г", "бакалея", "Мистраль", 139.00),
    ("P009", "Гречка 900г", "бакалея", "Мистраль", 129.00),
    ("P010", "Кофе молотый 250г", "бакалея", "Jardin", 329.00),
    ("P011", "Чай чёрный 100 пак.", "бакалея", "Tess", 289.00),
    ("P012", "Кола 1.5л", "напитки", "Pepsi", 139.00),
    ("P013", "Вода без газа 1.5л", "напитки", "Аква Минерале", 45.00),
    ("P014", "Сок апельсиновый 1л", "напитки", "Добрый", 149.00),
    ("P015", "Шоколад молочный 90г", "снеки", "Alpen Gold", 99.00),
    ("P016", "Печенье юбилейное", "снеки", "Юбилейное", 89.00),
    ("P017", "Чипсы со вкусом сыра", "снеки", "Lays", 149.00),
    ("P018", "Порошок стиральный 3кг", "хозтовары", "Tide", 449.00),
    ("P019", "Мыло жидкое 500мл", "хозтовары", "AOS", 179.00),
    ("P020", "Салфетки бумажные", "хозтовары", "Соффия", 89.00),
]


def customer_pool(day: str) -> int:
    """Сколько карт лояльности уже "выпущено" на дату. Детерминировано."""
    base = datetime(2026, 11, 1)
    idx = (datetime.strptime(day, "%Y-%m-%d") - base).days
    return 50 + max(idx, 0) * 10


def handler(event, context):
    """Имитация выгрузки касс: ежедневный CSV в staging."""
    day = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    rnd = random.Random(f"pos-{day}")
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "https://storage.yandexcloud.net"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="ru-central1",
    )
    bucket = os.environ["S3_BUCKET"]

    stores = [1, 2, 3, 4, 5]
    pool = customer_pool(day)
    rows = []
    for t in range(1, rnd.randint(120, 180) + 1):
        tx_id = "TX" + day.replace("-", "") + f"{t:05d}"
        store = rnd.choice(stores)
        dt = datetime.strptime(day, "%Y-%m-%d") + timedelta(
            hours=rnd.randint(9, 21), minutes=rnd.randint(0, 59))
        card = f"LC{rnd.randint(1, pool):06d}" if rnd.random() < 0.45 else ""
        for _ in range(rnd.randint(1, 6)):
            sku, _name, _cat, _brand, price = PRODUCTS[rnd.randrange(len(PRODUCTS))]
            rows.append([tx_id, store, rnd.randint(1, 20),
                         dt.strftime("%Y-%m-%d %H:%M:%S"), sku,
                         rnd.randint(1, 5), f"{price:.2f}",
                         rnd.choice(["card", "cash"]), card])

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["transaction_id", "store_id", "cashier_id", "event_datetime",
                "sku", "quantity", "price", "payment_type", "loyalty_card"])
    w.writerows(rows)

    key = f"pos_sales/date={day}/part-000.csv"
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue().encode("utf-8"))
    return {"statusCode": 200, "body": f"written {key}, rows={len(rows)}"}
