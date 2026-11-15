import json
import os
import random
from datetime import datetime, timedelta, timezone

import boto3

PRODUCTS = [
    ("P001", "Молоко 2.5% 1л", 89.90), ("P002", "Кефир 1% 1л", 95.50),
    ("P003", "Сметана 20% 400г", 129.00), ("P004", "Творог 5% 300г", 149.90),
    ("P005", "Сыр Гауда 200г", 219.00), ("P006", "Батон нарезной", 42.00),
    ("P007", "Спагетти 450г", 119.00), ("P008", "Рис круглозерный 900г", 139.00),
    ("P009", "Гречка 900г", 129.00), ("P010", "Кофе молотый 250г", 329.00),
    ("P011", "Чай чёрный 100 пак.", 289.00), ("P012", "Кола 1.5л", 139.00),
    ("P013", "Вода без газа 1.5л", 45.00), ("P014", "Сок апельсиновый 1л", 149.00),
    ("P015", "Шоколад молочный 90г", 99.00), ("P016", "Печенье юбилейное", 89.00),
    ("P017", "Чипсы со вкусом сыра", 149.00), ("P018", "Порошок стиральный 3кг", 449.00),
    ("P019", "Мыло жидкое 500мл", 179.00), ("P020", "Салфетки бумажные", 89.00),
]


def customer_pool(day: str) -> int:
    base = datetime(2026, 11, 1)
    idx = (datetime.strptime(day, "%Y-%m-%d") - base).days
    return 50 + max(idx, 0) * 10


def handler(event, context):
    """Имитация выгрузки заказов интернет-магазина: JSONEachRow в staging."""
    day = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    rnd = random.Random(f"online-{day}")
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "https://storage.yandexcloud.net"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="ru-central1",
    )
    bucket = os.environ["S3_BUCKET"]

    pool = customer_pool(day)
    base_dt = datetime.strptime(day, "%Y-%m-%d")
    lines = []
    for n in range(1, rnd.randint(80, 150) + 1):
        oid = f"ORD{day.replace('-', '')}{n:05d}"
        cust = rnd.randint(1, pool) if rnd.random() < 0.6 else 0
        created = base_dt + timedelta(hours=rnd.randint(0, 23),
                                      minutes=rnd.randint(0, 59),
                                      seconds=rnd.randint(0, 59))
        delivery = rnd.choice(["courier", "pickup"])
        for _ in range(rnd.randint(1, 4)):
            sku, _name, price = PRODUCTS[rnd.randrange(len(PRODUCTS))]
            qty = rnd.randint(1, 4)
            disc = round(price * qty * rnd.choice([0.0, 0.0, 0.05, 0.10]), 2)
            lines.append({
                "order_id": oid,
                "customer_id": cust,
                "sku": sku,
                "quantity": qty,
                "price": round(price, 2),
                "discount": disc,
                "created_at": created.strftime("%Y-%m-%d %H:%M:%S"),
                "delivery_type": delivery,
            })

    body = "\n".join(json.dumps(o, ensure_ascii=False) for o in lines)
    key = f"online_orders/date={day}/orders.json"
    s3.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
    return {"statusCode": 200, "body": f"written {key}, lines={len(lines)}"}
