import io
import os
import random
from datetime import datetime, timedelta, timezone

import boto3


def customer_pool(day: str) -> int:
    base = datetime(2026, 11, 1)
    idx = (datetime.strptime(day, "%Y-%m-%d") - base).days
    return 50 + max(idx, 0) * 10


def handler(event, context):
    """Имитация выгрузки программы лояльности: события register/purchase/redeem."""
    day = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    rnd = random.Random(f"loyalty-{day}")
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
    events = []
    n = 0

    def add(card, cust, etype, points):
        nonlocal n
        n += 1
        dt = base_dt + timedelta(hours=rnd.randint(0, 23), minutes=rnd.randint(0, 59))
        events.append([f"EV{day.replace('-', '')}{n:05d}",
                       dt.strftime("%Y-%m-%d %H:%M:%S"), card, cust, etype, points])

    # Ежедневно выпускаем 10 новых карт (ленивая регистрация, детерминировано по дате)
    for i in range(pool - 9, pool + 1):
        add(f"LC{i:06d}", i, "register", 0)

    for _ in range(rnd.randint(30, 60)):
        i = rnd.randint(1, pool)
        add(f"LC{i:06d}", i, "purchase", rnd.randint(50, 500))

    for _ in range(rnd.randint(5, 15)):
        i = rnd.randint(1, pool)
        add(f"LC{i:06d}", i, "redeem", -rnd.randint(50, 200))

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["event_id", "event_datetime", "card_id", "customer_id", "event_type", "points"])
    w.writerows(events)

    key = f"loyalty/date={day}/events.csv"
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue().encode("utf-8"))
    return {"statusCode": 200, "body": f"written {key}, events={len(events)}"}
