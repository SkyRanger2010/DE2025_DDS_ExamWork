# Data Vault on TPC-H (Trino)

## 1. Описание задачи

В рамках задания реализовано хранилище данных (DWH) по методологии **Data Vault 2.0**
на основе датасета **TPC-H (schema: tpch.tiny)** с использованием **Trino**.

Реализация включает:
- проектирование хранилища в модели **HUB / LINK / SAT**
- поддержку **SCD Type 2** для спутников
- параметризованную загрузку данных:
  - либо **всего датасета**
  - либо **только заказов за выбранную дату** и всех связанных с ними сущностей

---

## 2. Архитектура Data Vault

### HUB (бизнес-ключи)
- hub_region
- hub_nation
- hub_customer
- hub_supplier
- hub_part
- hub_order

### LINK (связи)
- lnk_nation_region
- lnk_customer_nation
- lnk_supplier_nation
- lnk_order_customer
- lnk_part_supplier
- lnk_lineitem (между hub_order и lnk_part_supplier)

### SATELLITE (атрибуты, SCD2)
- sat_region
- sat_nation
- sat_customer (редко меняющиеся признаки)
- sat_customer_balance (часто меняющийся баланс)
- sat_supplier (редко меняющиеся признаки)
- sat_supplier_balance (часто меняющийся баланс)
- sat_part
- sat_order (редко меняющиеся признаки)
- sat_order_status (часто меняющийся статус)
- sat_part_supplier
- sat_lineitem (редко меняющиеся признаки)
- sat_lineitem_status (часто меняющийся статус)

---

## 3. Подготовка окружения Trino

### 3.1 Развертывание Trino (Docker)

```bash
docker run -d \
  --name trino \
  -p 8080:8080 \
  trinodb/trino
```

Web UI:
```
http://localhost:8080
```

CLI:
```bash
docker exec -it trino trino
```

---

### 3.2 Каталоги и схемы

Используемые каталоги:
- tpch — источник данных
- memory — учебное хранилище Data Vault

Создание схемы:
```sql
CREATE SCHEMA IF NOT EXISTS memory.data_vault;
USE memory.data_vault;
```

---

### 3.3 Источник данных TPC-H

Схема:
```sql
tpch.tiny
```

Таблицы:
- orders
- lineitem
- customer
- supplier
- part
- partsupp
- nation
- region

---

### 3.4 Ограничения memory connector

- UPDATE / DELETE / MERGE не поддерживаются
- Только CREATE TABLE и INSERT
- Данные не персистентны

SCD2 реализован как insert-only.

---

## 4. Параметризация загрузки

Используется таблица dv_run_control.

Параметр load_date:
- NULL → загрузка всего датасета
- DATE → загрузка заказов за дату и всех связанных сущностей

---

## 5. Логика ETL

1. Материализация src_* таблиц с фильтрацией по дате
2. Представления v_src_* с расчетом HK / HLK / HASHDIFF
3. Загрузка HUB (insert-only)
4. Загрузка LINK (insert-only)
5. Загрузка SAT (SCD2 by query)

---

## 6. Структура файлов решения

- DDL.sql — DDL Data Vault
- load_all_data.sql — скрипт загрузки всего датасета
- load_by_date.sql — скрипт с параметром (загрузка по дате или полная)
- ER.pdf — ER-диаграмма DATA VAULT
- mart_example.sql — скрипт примера построения витрины на основе DATA VAULT
- README.md — описание решения

---

## 7. Пример витрины построенной на основе DATA VAULT

| region        | customer_country | customer_name         | orders_cnt | orders_amount |
|--------------|------------------|-----------------------|------------|---------------|
| MIDDLE EAST  | EGYPT            | Customer#000000902    | 6          | 946489.0      |
| ASIA         | VIETNAM          | Customer#000001439    | 9          | 1548129.0     |
| AMERICA      | UNITED STATES    | Customer#000000689    | 13         | 1788307.0     |
| AFRICA       | ALGERIA          | Customer#000000823    | 22         | 2551940.0     |
| ASIA         | JAPAN            | Customer#000001306    | 18         | 2263960.0     |
| AMERICA      | UNITED STATES    | Customer#000000676    | 21         | 3601569.0     |
| AFRICA       | ALGERIA          | Customer#000000295    | 19         | 3197184.0     |
| AMERICA      | BRAZIL           | Customer#000000487    | 27         | 4092893.0     |
| EUROPE       | GERMANY          | Customer#000000922    | 15         | 1865767.0     |
| EUROPE       | FRANCE           | Customer#000001477    | 20         | 3224037.0     |

Скрипт витрины:
```sql
-- ==========================================================================
--                ПРИМЕР ВИТРИНЫ
-- Количество и общая сумма заказов с группировкой (заказчик, страна, регион)
-- ==========================================================================

USE memory.data_vault;

DROP TABLE IF EXISTS mart_orders_by_geo_customer;

CREATE TABLE mart_orders_by_geo_customer AS
WITH
sat_order_last AS (
  SELECT s.*
  FROM sat_order s
  JOIN (
    SELECT hk_order, max(valid_from) AS max_vf
    FROM sat_order
    GROUP BY hk_order
  ) x ON x.hk_order = s.hk_order AND x.max_vf = s.valid_from
  WHERE coalesce(s.is_deleted, false) = false
),
sat_customer_last AS (
  SELECT s.*
  FROM sat_customer s
  JOIN (
    SELECT hk_customer, max(valid_from) AS max_vf
    FROM sat_customer
    GROUP BY hk_customer
  ) x ON x.hk_customer = s.hk_customer AND x.max_vf = s.valid_from
  WHERE coalesce(s.is_deleted, false) = false
),
sat_nation_last AS (
  SELECT s.*
  FROM sat_nation s
  JOIN (
    SELECT hk_nation, max(valid_from) AS max_vf
    FROM sat_nation
    GROUP BY hk_nation
  ) x ON x.hk_nation = s.hk_nation AND x.max_vf = s.valid_from
  WHERE coalesce(s.is_deleted, false) = false
),
sat_region_last AS (
  SELECT s.*
  FROM sat_region s
  JOIN (
    SELECT hk_region, max(valid_from) AS max_vf
    FROM sat_region
    GROUP BY hk_region
  ) x ON x.hk_region = s.hk_region AND x.max_vf = s.valid_from
  WHERE coalesce(s.is_deleted, false) = false
)
SELECT
  r.r_name                    	AS region,
  n.n_name                    	AS customer_country,
  c.c_name                    	AS customer_name,
  count(DISTINCT o.hk_order) 	AS orders_cnt,
  round(sum(o.o_totalprice),0)	AS orders_amount
FROM hub_order ho
JOIN sat_order_last o
  ON o.hk_order = ho.hk_order
JOIN lnk_order_customer loc
  ON loc.hk_order = ho.hk_order
JOIN sat_customer_last c
  ON c.hk_customer = loc.hk_customer
JOIN lnk_customer_nation lcn
  ON lcn.hk_customer = c.hk_customer
JOIN sat_nation_last n
  ON n.hk_nation = lcn.hk_nation
JOIN lnk_nation_region lnr
  ON lnr.hk_nation = n.hk_nation
JOIN sat_region_last r
  ON r.hk_region = lnr.hk_region
GROUP BY
  r.r_name, n.n_name, c.c_name;

```

---
