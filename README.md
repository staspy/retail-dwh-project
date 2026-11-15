# DWH "Розница" — Yandex Cloud

Data Warehouse для сети магазинов: данные касс, программы лояльности и интернет-магазина
выгружаются в Object Storage (staging), Airflow переносит и трансформирует их в ClickHouse
(схема "звезда"), визуализация — DataLens. Сбои DAG алертятся в Tracker.

## Архитектура

```
Cloud Functions (генераторы данных)
        |  ежедневно в 04:00 UTC
        v
Object Storage: retail-staging-XXX/            <-- staging-слой
   pos_sales/date=YYYY-MM-DD/*.csv             (касса)
   loyalty/date=YYYY-MM-DD/events.csv          (лояльность)
   online_orders/date=YYYY-MM-DD/orders.json   (интернет-магазин)
        |
        v
Managed Airflow: DAG retail_dwh_etl (ежедневно в 06:00 UTC)
   check_staging_files -> load_raw -> dq_checks -> load_dims
                        -> load_fact -> optimize_dims -> archive_staging
        |
        v
Managed ClickHouse
   raw.pos_sales, raw.loyalty, raw.online_orders          <- сырой слой
   dwh.fact_sales, dwh.fact_loyalty_events                <- факты
   dwh.dim_products, dim_stores, dim_customers, dim_channels, dim_date  <- измерения
        |
        v
DataLens: дашборд "Сводный отчёт по сети"
```

Мониторинг: логи функций — Cloud Logging (группа по умолчанию), логи Airflow — бакет DAGS,
при падении задачи DAG создаёт тикет в Tracker (callback on_failure_callback).

## Файлы проекта

| Файл | Назначение |
|---|---|
| `00_setup_infra.sh` | Сеть, NAT-шлюз, группа безопасности, сервисные аккаунты, бакеты, кластеры ClickHouse и Airflow. В конце создаёт `creds.env` |
| `01_dwh_schema.sql` | Слой raw + таблицы звезды + справочные данные + `dwh.dq_results` |
| `02_setup_functions.sh` | Деплой 3 функций-генераторов и таймер-триггеров |
| `functions/*/index.py` | Генераторы: касса, лояльность, интернет-магазин (детерминированные по дате) |
| `dags/retail_dwh_etl.py` | DAG ETL: check -> raw -> DQ -> dims -> fact -> optimize -> archive |

## Порядок запуска

1. Установите yc CLI и выполните `yc init` (если ещё не сделано).
2. `bash 00_setup_infra.sh` — ждите, кластер Airflow создаётся 10-20 минут.
   В конце появится файл `creds.env` со всеми паролями, ключами и именами бакетов.
3. Примените схему: откройте кластер ClickHouse в консоли -> WebSQL -> выполните
   содержимое `01_dwh_schema.sql`. Либо через CLI:
   ```
   clickhouse-client --host $CH_FQDN --secure --port 9440 \
     --user etl_user --ask-password --database dwh < 01_dwh_schema.sql
   ```
4. `bash 02_setup_functions.sh` — создаст функции и таймер-триггеры.
5. Ручной тест генераторов (каждый пишет файл за вчерашний день в staging):
   ```
   yc serverless function invoke --name gen-pos-sales
   yc serverless function invoke --name gen-loyalty
   yc serverless function invoke --name gen-online-orders
   ```
6. Загрузите DAG в бакет Airflow (папка `dags/`):
   ```
   source creds.env
   yc storage s3 cp dags/retail_dwh_etl.py s3://$DAGS_BUCKET/dags/retail_dwh_etl.py
   ```
7. Настройте Airflow UI (адрес из консоли: Managed Apache Airflow -> retail-airflow ->
   "Открыть интерфейс Airflow", логин `admin`):
   - **Connection** (Admin -> Connections -> +): id `clickhouse_dwh`,
     host = `$CH_FQDN` из creds.env, port `8443`, login `etl_user`,
     password = `$CH_ETL_PASSWORD`, schema `dwh`.
   - **Variables**: `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET` = `$STAGING_BUCKET`.
     Опционально: `TRACKER_ORG_ID`, `TRACKER_TOKEN`, `TRACKER_QUEUE`.
8. Включите DAG `retail_dwh_etl` и нажмите **Trigger**. Дождитесь зелёных задач.
9. Проверка результата в WebSQL:
   ```sql
   SELECT toDate(event_datetime) AS d, count() AS lines, round(sum(amount)) AS revenue
   FROM dwh.fact_sales GROUP BY d ORDER BY d;
   SELECT * FROM dwh.dq_results ORDER BY ds DESC LIMIT 20;
   ```

## Схема DWH (звезда)

- **Факты**: `fact_sales` (продажи кассы и онлайн, `channel_id` различает каналы),
  `fact_loyalty_events` (регистрация карт, начисления и списания бонусов).
- **Измерения**: `dim_products`, `dim_stores`, `dim_customers`, `dim_channels`, `dim_date`.
- `Nullable` у `store_id` и `customer_id` — онлайн-заказ может быть без магазина и без карты.
- Измерения — `ReplacingMergeTree`: при перезагрузке справочника дубли схлопываются,
  в DAG после загрузки выполняется `OPTIMIZE ... FINAL`.
- Факты партиционированы по месяцам (`PARTITION BY toYYYYMM`), порядок ключей под типовые срезы.

## Проверки качества (задача dq_checks)

- полнота: все три источника за дату непустые;
- уникальность: `line_id` факта не дублируются;
- согласованность ключей: sku, store_id, customer_id из факта есть в измерениях;
- бизнес-валидации: quantity > 0, amount > 0, sku непустой;
- результаты пишутся в `dwh.dq_results`, при провале DAG останавливается до витрин.

## DataLens

1. Активируйте DataLens в том же каталоге.
2. Страница кластера ClickHouse -> вкладка DataLens -> создать подключение (пользователь `analyst`,
   доступ из DataLens включён на уровне кластера). Вручную указывать хост не нужно.
3. Создайте воркбук, датасеты: `fact_sales` + `dim_date`, `dim_channels`, `dim_products`,
   `dim_customers`, `fact_loyalty_events`.
4. Чарты: выручка по дням; выручка по каналам (offline/online); топ-10 товаров по выручке;
   доля чеков с картой лояльности; средний чек; начисленные и списанные бонусы.
5. Соберите всё на дашборд "Сводный отчёт по сети" с фильтрами по дате, каналу и городу.

## Стоимость и уборка

Кластеры ClickHouse и Airflow тарифицируются повременно. После того, как проект уже не нужен, необходимо удалить
кластеры (сначала снимите deletion-protection) и бакеты — иначе счёт будет расти.
