-- ============================================================
-- Учебный DWH "Розница". Слой RAW + слой DWH (звезда).
-- Выполнять на кластере ClickHouse от пользователя etl_user.
-- ============================================================

-- ---------------------- Слой RAW (как в staging) ----------------------
CREATE TABLE IF NOT EXISTS raw.pos_sales (
    transaction_id String,
    store_id       UInt16,
    cashier_id     UInt16,
    event_datetime DateTime,
    sku            String,
    quantity       UInt32,
    price          Decimal(10, 2),
    payment_type   LowCardinality(String),
    loyalty_card   String
) ENGINE = MergeTree ORDER BY (event_datetime, transaction_id);

CREATE TABLE IF NOT EXISTS raw.loyalty (
    event_id       String,
    event_datetime DateTime,
    card_id        String,
    customer_id    UInt32,
    event_type     LowCardinality(String),
    points         Int32
) ENGINE = MergeTree ORDER BY (event_datetime, event_id);

CREATE TABLE IF NOT EXISTS raw.online_orders (
    order_id      String,
    customer_id   UInt32,
    sku           String,
    quantity      UInt32,
    price         Decimal(10, 2),
    discount      Decimal(10, 2),
    created_at    DateTime,
    delivery_type LowCardinality(String)
) ENGINE = MergeTree ORDER BY (created_at, order_id);

-- ---------------------- Измерения (звезда) ----------------------

CREATE TABLE IF NOT EXISTS dwh.dim_products (
    sku        String,
    name       String,
    category   LowCardinality(String),
    brand      LowCardinality(String),
    price      Decimal(10, 2),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY sku;

CREATE TABLE IF NOT EXISTS dwh.dim_stores (
    store_id     UInt16,
    city         String,
    store_format LowCardinality(String),
    opened_at    Date
) ENGINE = ReplacingMergeTree ORDER BY store_id;

CREATE TABLE IF NOT EXISTS dwh.dim_channels (
    channel_id   UInt8,
    channel_name LowCardinality(String)
) ENGINE = MergeTree ORDER BY channel_id;

CREATE TABLE IF NOT EXISTS dwh.dim_customers (
    customer_id   UInt64,
    card_id       String,
    segment       LowCardinality(String),
    registered_at DateTime,
    updated_at    DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key    Date,
    day_of_week UInt8,
    month       UInt8,
    quarter     UInt8,
    is_weekend  UInt8
) ENGINE = MergeTree ORDER BY date_key;

-- ---------------------- Факты ----------------------

CREATE TABLE IF NOT EXISTS dwh.fact_sales (
    line_id       String,
    date_key      Date,
    event_datetime DateTime,
    store_id      Nullable(UInt16),
    customer_id   Nullable(UInt64),
    sku           String,
    channel_id    UInt8,
    quantity      UInt32,
    price         Decimal(10, 2),
    discount      Decimal(10, 2) DEFAULT 0,
    amount        Decimal(12, 2)
) ENGINE = MergeTree 
PARTITION BY toYYYYMM(date_key) 
ORDER BY (date_key, store_id, sku)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS dwh.fact_loyalty_events (
    event_id       String,
    event_datetime DateTime,
    card_id        String,
    customer_id    UInt32,
    event_type     LowCardinality(String),
    points         Int32
) ENGINE = MergeTree ORDER BY (event_datetime, event_id);

-- Результаты проверок качества
CREATE TABLE IF NOT EXISTS dwh.dq_results (
    check_name String,
    ds         Date,
    status     LowCardinality(String),
    details    String
) ENGINE = MergeTree ORDER BY (ds, check_name);

-- ---------------------- Справочные данные ----------------------

INSERT INTO dwh.dim_products (sku, name, category, brand, price) VALUES
('P001','Молоко 2.5% 1л','молочные','Простоквашино',89.90),
('P002','Кефир 1% 1л','молочные','Домик в деревне',95.50),
('P003','Сметана 20% 400г','молочные','Простоквашино',129.00),
('P004','Творог 5% 300г','молочные','Домик в деревне',149.90),
('P005','Сыр Гауда 200г','молочные','Ламбер',219.00),
('P006','Батон нарезной','бакалея','Хлебный дом',42.00),
('P007','Спагетти 450г','бакалея','Barilla',119.00),
('P008','Рис круглозерный 900г','бакалея','Мистраль',139.00),
('P009','Гречка 900г','бакалея','Мистраль',129.00),
('P010','Кофе молотый 250г','бакалея','Jardin',329.00),
('P011','Чай чёрный 100 пак.','бакалея','Tess',289.00),
('P012','Кола 1.5л','напитки','Pepsi',139.00),
('P013','Вода без газа 1.5л','напитки','Аква Минерале',45.00),
('P014','Сок апельсиновый 1л','напитки','Добрый',149.00),
('P015','Шоколад молочный 90г','снеки','Alpen Gold',99.00),
('P016','Печенье юбилейное','снеки','Юбилейное',89.00),
('P017','Чипсы со вкусом сыра','снеки','Lays',149.00),
('P018','Порошок стиральный 3кг','хозтовары','Tide',449.00),
('P019','Мыло жидкое 500мл','хозтовары','AOS',179.00),
('P020','Салфетки бумажные','хозтовары','Соффия',89.00);

INSERT INTO dwh.dim_stores (store_id, city, store_format, opened_at) VALUES
(1,'Краснодар','гипермаркет','2019-03-01'),
(2,'Краснодар','супермаркет','2021-07-15'),
(3,'Сочи','супермаркет','2020-05-10'),
(4,'Ростов-на-Дону','гипермаркет','2018-11-20'),
(5,'Ставрополь','супермаркет','2022-02-01');

INSERT INTO dwh.dim_channels (channel_id, channel_name) VALUES
(1,'offline'),
(2,'online');

INSERT INTO dwh.dim_date
SELECT d AS date_key,
       toDayOfWeek(d) AS day_of_week,
       toMonth(d) AS month,
       toQuarter(d) AS quarter,
       if(toDayOfWeek(d) IN (6, 7), 1, 0) AS is_weekend
FROM (SELECT addDays(toDate('2024-01-01'), number) AS d FROM numbers(1826));
