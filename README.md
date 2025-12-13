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
- lnk_lineitem (Order + PartSupplier + LineNumber)

### SATELLITE (атрибуты, SCD2)
- sat_region
- sat_nation
- sat_customer
- sat_customer_balance
- sat_supplier
- sat_supplier_balance
- sat_part
- sat_order
- sat_order_status
- sat_part_supplier
- sat_lineitem
- sat_lineitem_status

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
- README.md — описание решения

---

