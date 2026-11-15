# -*- coding: utf-8 -*-
# DAG retail_dwh_etl: Object Storage (staging) -> ClickHouse (звезда)
# Логика: проверка staging -> загрузка raw -> проверки качества -> dims -> facts -> optimize -> архив

from datetime import timedelta

import pendulum
import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable

try:  # Airflow 3.x
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.python import PythonOperator

S3_ENDPOINT = "https://storage.yandexcloud.net"

SOURCES = {
    "pos": {
        "prefix": "pos_sales",
        "file_glob": "*.csv",
        "format": "CSVWithNames",
        "raw_table": "raw.pos_sales",
        "clear_sql": "ALTER TABLE raw.pos_sales DELETE WHERE toDate(event_datetime) = toDate(\'{ds}\')",
    },
    "loyalty": {
        "prefix": "loyalty",
        "file_glob": "*.csv",
        "format": "CSVWithNames",
        "raw_table": "raw.loyalty",
        "clear_sql": "ALTER TABLE raw.loyalty DELETE WHERE toDate(event_datetime) = toDate(\'{ds}\')",
    },
    "online": {
        "prefix": "online_orders",
        "file_glob": "*.json",
        "format": "JSONEachRow",
        "raw_table": "raw.online_orders",
        "clear_sql": "ALTER TABLE raw.online_orders DELETE WHERE toDate(created_at) = toDate(\'{ds}\')",
    },
}


def ch_query(sql: str) -> str:
    """Выполняет SQL на ClickHouse через HTTP (порт 8443, TLS)."""
    conn = BaseHook.get_connection("clickhouse_dwh")
    resp = requests.post(
        f"https://{conn.host}:{conn.port or 8443}/",
        params={"user": conn.login, "password": conn.password,
                "database": conn.schema or "dwh"},
        data=sql.encode("utf-8"),
        timeout=600,
    )
    if resp.status_code != 200:
        raise AirflowException(f"ClickHouse вернул {resp.status_code}: {resp.text[:800]}")
    return resp.text.strip()


def s3_client():
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=Variable.get("S3_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("S3_SECRET_KEY"),
        region_name="ru-central1",
    )


def check_staging(**context):
    """Есть ли выгрузки всех трёх источников за дату запуска?"""
    ds = context["ds"]
    bucket = Variable.get("S3_BUCKET")
    s3 = s3_client()
    missing = []
    for name, src in SOURCES.items():
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=f"{src['prefix']}/date={ds}/")
        found = resp.get("KeyCount", 0)
        print(f"[staging] {name}: объектов = {found}")
        if found == 0:
            missing.append(name)
    if missing:
        raise AirflowException(f"Нет выгрузок в staging за {ds}: " + ", ".join(missing))


def load_raw(**context):
    """Перенос файлов из staging в слой raw (идемпотентно: сначала чистим день)."""
    ds = context["ds"]
    bucket = Variable.get("S3_BUCKET")
    ak = Variable.get("S3_ACCESS_KEY")
    sk = Variable.get("S3_SECRET_KEY")
    for name, src in SOURCES.items():
        ch_query(src["clear_sql"].format(ds=ds))
        path = f"{S3_ENDPOINT}/{bucket}/{src['prefix']}/date={ds}/{src['file_glob']}"
        sql = (f"INSERT INTO {src['raw_table']} "
               f"SELECT * FROM s3('{path}', '{ak}', '{sk}', '{src['format']}')")
        ch_query(sql)
        print(f"[raw] {name}: загружено за {ds}")


def load_dims(**context):
    """Справочник покупателей: register-события лояльности + покупатели онлайн."""
    ch_query(f"""
        INSERT INTO dwh.dim_customers (customer_id, card_id, segment, registered_at, updated_at)
        SELECT customer_id,
               argMaxIf(card_id, event_datetime, card_id != ''),
               'loyalty',
               min(event_datetime),
               now()
        FROM raw.loyalty
        WHERE customer_id > 0
        GROUP BY customer_id
    """)
    ch_query(f"""
        INSERT INTO dwh.dim_customers (customer_id, card_id, segment, registered_at, updated_at)
        SELECT customer_id, '', 'online', min(created_at), now()
        FROM raw.online_orders
        WHERE customer_id > 0
          AND customer_id NOT IN (SELECT customer_id FROM dwh.dim_customers)
        GROUP BY customer_id
    """)
    print("[dims] dim_customers дополнен")


def load_fact(**context):
    """Сборка фактов: продажи (касса + онлайн) и события лояльности."""
    ds = context["ds"]
    ch_query(f"ALTER TABLE dwh.fact_sales DELETE WHERE date_key = toDate('{ds}')")
    ch_query(f"ALTER TABLE dwh.fact_loyalty_events DELETE WHERE toDate(event_datetime) = toDate('{ds}')")

    # Продажи в магазинах (канал offline)
    ch_query(f"""
        INSERT INTO dwh.fact_sales
        SELECT concat('pos-', transaction_id, '-', sku) AS line_id,
               toDate(event_datetime) AS date_key,
               event_datetime,
               toUInt16(store_id) AS store_id,
               if(loyalty_card != '', toUInt64(replaceOne(loyalty_card, 'LC', '')), NULL) AS customer_id,
               sku,
               1 AS channel_id,
               quantity,
               price,
               toDecimal32(0, 2) AS discount,
               toDecimal64(quantity * price, 2) AS amount
        FROM raw.pos_sales
        WHERE toDate(event_datetime) = toDate('{ds}')
    """)

    # Заказы интернет-магазина (канал online)
    ch_query(f"""
        INSERT INTO dwh.fact_sales
        SELECT concat('onl-', order_id, '-', sku) AS line_id,
               toDate(created_at),
               created_at,
               NULL,
               if(customer_id > 0, toUInt64(customer_id), NULL),
               sku,
               2 AS channel_id,
               quantity,
               price,
               discount,
               toDecimal64(quantity * price - discount, 2)
        FROM raw.online_orders
        WHERE toDate(created_at) = toDate('{ds}')
    """)

    # События лояльности
    ch_query(f"""
        INSERT INTO dwh.fact_loyalty_events
        SELECT event_id, event_datetime, card_id, customer_id, event_type, points
        FROM raw.loyalty
        WHERE toDate(event_datetime) = toDate('{ds}')
    """)
    print(f"[fact] факты за {ds} загружены")


CHECKS = [
    ("completeness_pos",
     "SELECT count() FROM raw.pos_sales WHERE toDate(event_datetime) = toDate('{ds}')", "NONZERO"),
    ("completeness_loyalty",
     "SELECT count() FROM raw.loyalty WHERE toDate(event_datetime) = toDate('{ds}')", "NONZERO"),
    ("completeness_online",
     "SELECT count() FROM raw.online_orders WHERE toDate(created_at) = toDate('{ds}')", "NONZERO"),
    ("unique_fact_lines",
     "SELECT count() FROM (SELECT line_id FROM dwh.fact_sales WHERE date_key = toDate('{ds}') "
     "GROUP BY line_id HAVING count() > 1)", "ZERO"),
    ("referential_products",
     "SELECT count() FROM dwh.fact_sales f LEFT JOIN dwh.dim_products p ON f.sku = p.sku "
     "WHERE f.date_key = toDate('{ds}') AND p.sku = ''", "ZERO"),
    ("referential_stores",
     "SELECT count() FROM dwh.fact_sales f LEFT JOIN dwh.dim_stores s ON f.store_id = s.store_id "
     "WHERE f.date_key = toDate('{ds}') AND f.store_id IS NOT NULL AND s.store_id = 0", "ZERO"),
    ("referential_customers",
     "SELECT count() FROM dwh.fact_sales f LEFT JOIN dwh.dim_customers c ON f.customer_id = c.customer_id "
     "WHERE f.date_key = toDate('{ds}') AND f.customer_id IS NOT NULL AND c.customer_id = 0", "ZERO"),
    ("business_amount_positive",
     "SELECT count() FROM dwh.fact_sales WHERE date_key = toDate('{ds}') AND (quantity = 0 OR amount <= 0)", "ZERO"),
    ("not_null_sku",
     "SELECT count() FROM dwh.fact_sales WHERE date_key = toDate('{ds}') AND sku = ''", "ZERO"),
]


def run_dq_checks(**context):
    """Проверки полноты, уникальности и согласованности ключей. Результаты — в dwh.dq_results."""
    ds = context["ds"]
    failures = []
    for name, sql, expectation in CHECKS:
        value = int(ch_query(sql.format(ds=ds)) or 0)
        ok = value > 0 if expectation == "NONZERO" else value == 0
        status = "PASS" if ok else "FAIL"
        details = f"value={value}, expected={expectation}"
        ch_query(f"INSERT INTO dwh.dq_results VALUES ('{name}', toDate('{ds}'), "
                 f"'{status}', '{details}')")
        print(f"[dq] {name}: {status} ({details})")
        if not ok:
            failures.append(f"{name}: {details}")
    if failures:
        raise AirflowException("Проверки качества не пройдены: " + "; ".join(failures))


def optimize_dims(**context):
    """Схлопывание дубликатов в измерениях (ReplacingMergeTree)."""
    for table in ("dwh.dim_customers", "dwh.dim_products", "dwh.dim_stores"):
        ch_query(f"OPTIMIZE TABLE {table} FINAL")
    print("[optimize] измерения сжаты")


def archive_staging(**context):
    """Обработанные файлы уезжают в processed/ — чтобы DAG был идемпотентным."""
    ds = context["ds"]
    bucket = Variable.get("S3_BUCKET")
    s3 = s3_client()
    for name, src in SOURCES.items():
        prefix = f"{src['prefix']}/date={ds}/"
        for obj in s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", []):
            dest = f"processed/{src['prefix']}/date={ds}/{obj['Key'].split('/')[-1]}"
            s3.copy_object(Bucket=bucket, Key=dest, CopySource={"Bucket": bucket, "Key": obj["Key"]})
            s3.delete_object(Bucket=bucket, Key=obj["Key"])
        print(f"[archive] {name}: файлы перемещены в processed/")


def notify_tracker(context):
    """Алерт в Tracker при падении задачи. Опционально: задайте Variables
    TRACKER_ORG_ID, TRACKER_TOKEN, TRACKER_QUEUE — без них просто пишет в лог."""
    try:
        org = Variable.get("TRACKER_ORG_ID", default_var=None)
        token = Variable.get("TRACKER_TOKEN", default_var=None)
        if not (org and token):
            print("Tracker не настроен (нет TRACKER_ORG_ID/TRACKER_TOKEN) — алерт пропущен.")
            return
        task = context["task_instance"].task_id
        payload = {
            "summary": f"[DWH] DAG retail_dwh_etl: упала задача {task}",
            "queue": {"id": Variable.get("TRACKER_QUEUE", default_var="DWH")},
            "type": {"name": "Task"},
            "description": f"run_id: {context['run_id']}\nОшибка: {context.get('exception')}",
        }
        requests.post(
            "https://api.tracker.yandex.net/v2/issues/",
            headers={"X-Org-Id": org, "Authorization": f"OAuth {token}"},
            json=payload,
            timeout=30,
        )
        print("Тикет в Tracker создан.")
    except Exception as e:  # алерт не должен ронять DAG повторно
        print(f"Не удалось создать тикет в Tracker: {e}")


with DAG(
    dag_id="retail_dwh_etl",
    description="ETL: Object Storage -> ClickHouse (звезда)",
    schedule="0 6 * * *",  # ежедневно в 06:00 UTC, обрабатывает вчерашний день
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "dwh",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": notify_tracker,
    },
    tags=["retail", "dwh"],
) as dag:
    check = PythonOperator(task_id="check_staging_files", python_callable=check_staging)
    load = PythonOperator(task_id="load_raw", python_callable=load_raw)
    dq = PythonOperator(task_id="dq_checks", python_callable=run_dq_checks)
    dims = PythonOperator(task_id="load_dims", python_callable=load_dims)
    fact = PythonOperator(task_id="load_fact", python_callable=load_fact)
    opt = PythonOperator(task_id="optimize_dims", python_callable=optimize_dims)
    arch = PythonOperator(task_id="archive_staging", python_callable=archive_staging)

    check >> load >> dq >> dims >> fact >> opt >> arch
