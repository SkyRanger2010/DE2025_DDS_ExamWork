-- =====================================================================
-- DDS / RAW VAULT: TPC-H -> Data Vault (memory.data_vault)
-- Назначение:
--   HUB  — хранит стабильные бизнес-ключи (BK) и их хэш-ключи (HK)
--   LINK — хранит связи между хабами (или link↔hub) через хэш-ключи (HLK)
--   SAT  — хранит описательные атрибуты и историю (SCD2) через HASHDIFF
--
-- Алгоритмы для ключей:
--   HK_*  = md5( BK ) -> VARBINARY
--   HLK_* = md5( concat(to_hex(HK_1),'|',to_hex(HK_2),...) ) -> VARBINARY
--   HASHDIFF = md5( concat(attr1,'|',attr2,'|',...) ) -> VARBINARY
--
-- SCD2 в SAT:
--   valid_from — начало актуальности версии
--   valid_to   — конец актуальности версии (открытая = '9999-12-31')
--   load_dts   — технический момент загрузки записи
--   is_deleted — логическое удаление (для source delete / tombstone)
-- =====================================================================
DROP SCHEMA IF EXISTS memory.data_vault CASCADE;
CREATE SCHEMA memory.data_vault;
USE memory.data_vault;

-- =====================================================================
-- HUB_REGION / SAT_REGION  РЕГИОН
-- =====================================================================

CREATE TABLE hub_region (
  hk_region      VARBINARY,                     -- HK: md5(r_regionkey)
  region_key     BIGINT,                        -- BK: R_REGIONKEY (натуральный ключ региона)
  load_dts       TIMESTAMP(3) WITH TIME ZONE,   -- техническая дата загрузки HK
  record_source  VARCHAR                        -- источник данных (например 'tpch.tiny')
);

CREATE TABLE sat_region (
  hk_region     VARBINARY,                      -- ссылка на HUB_REGION (логически)
  load_dts      TIMESTAMP(3) WITH TIME ZONE,    -- момент загрузки версии SAT
  hashdiff      VARBINARY,                      -- md5(r_name|r_comment)
  r_name        VARCHAR,                        -- наименование региона
  r_comment     VARCHAR,                        -- комментарий
  record_source VARCHAR,                        -- источник данных
  valid_from    TIMESTAMP(3) WITH TIME ZONE,    -- начало актуальности
  valid_to      TIMESTAMP(3) WITH TIME ZONE,    -- конец актуальности (открытая версия = 9999-12-31)
  is_deleted    BOOLEAN                         -- признак логического удаления
);

-- =====================================================================
-- HUB_NATION / SAT_NATION   СТРАНА
-- =====================================================================

CREATE TABLE hub_nation (
  hk_nation      VARBINARY,                     -- HK: md5(n_nationkey)
  nation_key     BIGINT,                        -- BK: N_NATIONKEY
  load_dts       TIMESTAMP(3) WITH TIME ZONE,	-- техническая дата загрузки HK
  record_source  VARCHAR						-- источник данных (например 'tpch.tiny')
);

CREATE TABLE sat_nation (
  hk_nation     VARBINARY,                      -- ссылка на HUB_NATION
  load_dts      TIMESTAMP(3) WITH TIME ZONE,	-- момент загрузки версии SAT
  hashdiff      VARBINARY,                      -- md5(n_name|n_comment)
  n_name        VARCHAR,						-- наименование страны				
  n_comment     VARCHAR,						-- комментарий
  record_source VARCHAR,						-- источник данных
  valid_from    TIMESTAMP(3) WITH TIME ZONE,	-- начало актуальности
  valid_to      TIMESTAMP(3) WITH TIME ZONE,	-- конец актуальности (открытая версия = 9999-12-31)
  is_deleted    BOOLEAN							-- признак логического удаления
);


-- =====================================================================
-- HUB_CUSTOMER / SAT_CUSTOMER / SAT_CUSTOMER_BALANCE  ЗАКАЗЧИК
-- =====================================================================

CREATE TABLE hub_customer (
  hk_customer    VARBINARY,                     -- HK: md5(c_custkey)
  cust_key       BIGINT,                        -- BK: C_CUSTKEY
  load_dts       TIMESTAMP(3) WITH TIME ZONE,	-- техническая дата загрузки HK
  record_source  VARCHAR						-- источник данных (например 'tpch.tiny')
);

-- SAT_CUSTOMER: стабильные описательные атрибуты заказчика 
CREATE TABLE sat_customer (
  hk_customer   VARBINARY,                      -- ссылка на HUB_CUSTOMER
  load_dts      TIMESTAMP(3) WITH TIME ZONE,	-- момент загрузки версии SAT
  hashdiff      VARBINARY,                      -- md5(name|address|phone|mktsegment|comment)
  c_name        VARCHAR,						-- наименование заказчика
  c_address     VARCHAR,						-- адрес заказчика
  c_phone       VARCHAR,						-- телефон заказчика
  c_mktsegment  VARCHAR,						-- рыночный сегмент заказчика
  c_comment     VARCHAR,						-- комментарий
  record_source VARCHAR,						-- источник данных
  valid_from    TIMESTAMP(3) WITH TIME ZONE,	-- начало актуальности
  valid_to      TIMESTAMP(3) WITH TIME ZONE,	-- конец актуальности (открытая версия = 9999-12-31)
  is_deleted    BOOLEAN							-- признак логического удаления
);

-- SAT_CUSTOMER_BALANCE: баланс заказчика отдельным спутником (часто меняется)
CREATE TABLE sat_customer_balance (
  hk_customer   VARBINARY,                      -- ссылка на HUB_CUSTOMER
  load_dts      TIMESTAMP(3) WITH TIME ZONE,    -- момент загрузки версии SAT
  hashdiff      VARBINARY,                      -- md5(c_acctbal)
  c_acctbal     DOUBLE,							-- актуальный балланс счета заказчика
  record_source VARCHAR,						-- источник данных
  valid_from    TIMESTAMP(3) WITH TIME ZONE,	-- начало актуальности
  valid_to      TIMESTAMP(3) WITH TIME ZONE,	-- конец актуальности (открытая версия = 9999-12-31)
  is_deleted    BOOLEAN							-- признак логического удаления
);


-- =====================================================================
-- HUB_SUPPLIER / SAT_SUPPLIER / SAT_SUPPLIER_BALANCE   ПОСТАВЩИК
-- =====================================================================

CREATE TABLE hub_supplier (
  hk_supplier    VARBINARY,                     -- HK: md5(s_suppkey)
  supp_key       BIGINT,                        -- BK: S_SUPPKEY
  load_dts       TIMESTAMP(3) WITH TIME ZONE,	-- техническая дата загрузки HK
  record_source  VARCHAR						-- источник данных (например 'tpch.tiny')
);

-- SAT_SUPPLIER: стабильные описательные атрибуты поставщика 
CREATE TABLE sat_supplier (
  hk_supplier   VARBINARY,                      -- ссылка на HUB_SUPPLIER
  load_dts      TIMESTAMP(3) WITH TIME ZONE,    -- момент загрузки версии SAT
  hashdiff      VARBINARY,                      -- md5(name|address|phone|comment)
  s_name        VARCHAR,						-- наименование поставщика
  s_address     VARCHAR,						-- адрес поставщика
  s_phone       VARCHAR,						-- телефон поставщика
  s_comment     VARCHAR,						-- комментарий
  record_source VARCHAR,						-- источник данных
  valid_from    TIMESTAMP(3) WITH TIME ZONE,	-- начало актуальности
  valid_to      TIMESTAMP(3) WITH TIME ZONE,	-- конец актуальности (открытая версия = 9999-12-31)
  is_deleted    BOOLEAN							-- признак логического удаления
);

-- SAT_SUPPLIER_BALANCE: баланс поставщика отдельным спутником (часто меняется)
CREATE TABLE sat_supplier_balance (
  hk_supplier   VARBINARY,                      -- ссылка на HUB_SUPPLIER
  load_dts      TIMESTAMP(3) WITH TIME ZONE,	-- момент загрузки версии SAT
  hashdiff      VARBINARY,                      -- md5(s_acctbal)
  s_acctbal     DOUBLE,							-- актуальный балланс счета заказчика
  record_source VARCHAR,						-- источник данных
  valid_from    TIMESTAMP(3) WITH TIME ZONE,	-- начало актуальности
  valid_to      TIMESTAMP(3) WITH TIME ZONE,	-- конец актуальности (открытая версия = 9999-12-31)
  is_deleted    BOOLEAN							-- признак логического удаления
);

-- =====================================================================
-- HUB_PART / SAT_PART    ПАРТИЯ ТОВАРА
-- =====================================================================

CREATE TABLE hub_part (
  hk_part        VARBINARY,                     -- HK: md5(p_partkey)
  part_key       BIGINT,                        -- BK: P_PARTKEY
  load_dts       TIMESTAMP(3) WITH TIME ZONE,	-- техническая дата загрузки HK
  record_source  VARCHAR						-- источник данных (например 'tpch.tiny')
);

-- SAT_PART: описательные атрибуты партии товара
CREATE TABLE sat_part (
  hk_part       VARBINARY,                      -- ссылка на HUB_PART
  load_dts      TIMESTAMP(3) WITH TIME ZONE,	-- момент загрузки версии SAT
  hashdiff      VARBINARY,                      -- md5(описательные атрибуты part)
  p_name        VARCHAR,						-- наименование партии
  p_mfgr        VARCHAR,						-- наименование производителя
  p_brand       VARCHAR,						-- бренд
  p_type        VARCHAR,						-- тип
  p_size        INTEGER,						-- размер партии
  p_container   VARCHAR,						-- тип контейнера
  p_retailprice DOUBLE,							-- цена продажи
  p_comment     VARCHAR,						-- комментарий
  record_source VARCHAR,						-- источник данных
  valid_from    TIMESTAMP(3) WITH TIME ZONE,	-- начало актуальности
  valid_to      TIMESTAMP(3) WITH TIME ZONE,	-- конец актуальности (открытая версия = 9999-12-31)
  is_deleted    BOOLEAN							-- признак логического удаления
);


-- =====================================================================
-- HUB_ORDER / SAT_ORDER / SAT_ORDER_STATUS     ЗАКАЗ
-- =====================================================================

CREATE TABLE hub_order (
  hk_order       VARBINARY,                     -- HK: md5(o_orderkey)
  order_key      BIGINT,                        -- BK: O_ORDERKEY
  load_dts       TIMESTAMP(3) WITH TIME ZONE,	-- техническая дата загрузки HK
  record_source  VARCHAR						-- источник данных (например 'tpch.tiny')
);

-- SAT_ORDER: описательные атрибуты заказа 
CREATE TABLE sat_order (
  hk_order        VARBINARY,                    -- ссылка на HUB_ORDER
  load_dts        TIMESTAMP(3) WITH TIME ZONE,	-- момент загрузки версии SAT
  hashdiff        VARBINARY,                    -- md5(totalprice|orderdate|priority|clerk|shippriority|comment)
  o_totalprice    DOUBLE,						-- сумма заказа
  o_orderdate     DATE,							-- дата заказа
  o_orderpriority VARCHAR,						-- приоритет заказа
  o_clerk         VARCHAR,						-- менеджер
  o_shippriority  INTEGER,						-- приоритет доставки заказа
  o_comment       VARCHAR,						-- комментарий
  record_source   VARCHAR,						-- источник данных
  valid_from      TIMESTAMP(3) WITH TIME ZONE,	-- начало актуальности
  valid_to        TIMESTAMP(3) WITH TIME ZONE,	-- конец актуальности (открытая версия = 9999-12-31)
  is_deleted      BOOLEAN						-- признак логического удаления
);

-- SAT_ORDER_STATUS: статус заказа отдельно (часто меняется)
CREATE TABLE sat_order_status (
  hk_order        VARBINARY,                    -- ссылка на HUB_ORDER
  load_dts        TIMESTAMP(3) WITH TIME ZONE,	-- момент загрузки версии SAT
  hashdiff        VARBINARY,                    -- md5(o_orderstatus)
  o_orderstatus   VARCHAR,						-- статус заказа
  record_source   VARCHAR,						-- источник данных
  valid_from      TIMESTAMP(3) WITH TIME ZONE,	-- начало актуальности
  valid_to        TIMESTAMP(3) WITH TIME ZONE,	-- конец актуальности (открытая версия = 9999-12-31)
  is_deleted      BOOLEAN						-- признак логического удаления
);


-- =====================================================================
-- LNK_PART_SUPPLIER / SAT_PART_SUPPLIER  СВЯЗЬ ПОСТАВЩИК-ПАРТИЯ
-- =====================================================================

CREATE TABLE lnk_part_supplier (
  hlk_part_supplier VARBINARY,                  -- HLK: md5(hex(hk_part)|hex(hk_supplier))
  hk_part           VARBINARY,                  -- ссылка на HUB_PART
  hk_supplier       VARBINARY,                  -- ссылка на HUB_SUPPLIER
  load_dts          TIMESTAMP(3) WITH TIME ZONE,-- техническая дата загрузки HLK
  record_source     VARCHAR						-- источник данных (например 'tpch.tiny')
);

-- SAT_PART_SUPPLIER: описательные атрибуты связи поставщик-партия
CREATE TABLE sat_part_supplier (
  hlk_part_supplier VARBINARY,                  -- ссылка на LNK_PART_SUPPLIER
  load_dts          TIMESTAMP(3) WITH TIME ZONE,-- момент загрузки версии SAT
  hashdiff          VARBINARY,                  -- md5(availqty|supplycost|comment)
  ps_availqty       INTEGER,                    -- доступное количество
  ps_supplycost     DOUBLE,                     -- стоимость поставки
  ps_comment        VARCHAR,					-- комментарий
  record_source     VARCHAR,					-- источник данных
  valid_from        TIMESTAMP(3) WITH TIME ZONE,-- начало актуальности
  valid_to          TIMESTAMP(3) WITH TIME ZONE,-- конец актуальности (открытая версия = 9999-12-31)
  is_deleted        BOOLEAN						-- признак логического удаления
);


-- =====================================================================
-- LNK_ORDER_CUSTOMER  СВЯЗЬ ЗАКАЗ-ЗАКАЗЧИК
-- =====================================================================

CREATE TABLE lnk_order_customer (
  hlk_order_customer VARBINARY,                 	-- HLK: md5(hex(hk_order)|hex(hk_customer))
  hk_order           VARBINARY,						-- ссылка на HUB_ORDER
  hk_customer        VARBINARY,						-- ссылка на HUB_CUSTOMER
  load_dts           TIMESTAMP(3) WITH TIME ZONE,	-- техническая дата загрузки HLK
  record_source      VARCHAR						-- источник данных (например 'tpch.tiny')
);

-- =====================================================================
-- LNK_NATION_REGION  СВЯЗЬ СТРАНА-РЕГИОН
-- =====================================================================

CREATE TABLE lnk_nation_region (
  hlk_nation_region VARBINARY,                  	-- HLK: md5(hex(hk_nation)|hex(hk_region))
  hk_nation         VARBINARY,						-- ссылка на HUB_NATION
  hk_region         VARBINARY,						-- ссылка на HUB_REGION
  load_dts          TIMESTAMP(3) WITH TIME ZONE,	-- техническая дата загрузки HLK
  record_source     VARCHAR							-- источник данных (например 'tpch.tiny')
);

-- =====================================================================
-- LNK_CUSTOMER_NATION  СВЯЗЬ ЗАКАЗЧИК-СТРАНА
-- =====================================================================

CREATE TABLE lnk_customer_nation (
  hlk_customer_nation VARBINARY,                	-- HLK: md5(hex(hk_customer)|hex(hk_nation))
  hk_customer         VARBINARY,					-- ссылка на HUB_CUSTOMER
  hk_nation           VARBINARY,					-- ссылка на HUB_NATION
  load_dts            TIMESTAMP(3) WITH TIME ZONE,	-- техническая дата загрузки HLK
  record_source       VARCHAR						-- источник данных (например 'tpch.tiny')
);

-- =====================================================================
-- LNK_SUPPLIER_NATION  СВЯЗЬ ПОСТАВЩИК-СТРАНА
-- =====================================================================

CREATE TABLE lnk_supplier_nation (
  hlk_supplier_nation VARBINARY,                	-- HLK: md5(hex(hk_supplier)|hex(hk_nation))
  hk_supplier         VARBINARY,					-- ссылка на HUB_SUPPLIER
  hk_nation           VARBINARY,					-- ссылка на HUB_NATION
  load_dts            TIMESTAMP(3) WITH TIME ZONE,	-- техническая дата загрузки HLK
  record_source       VARCHAR						-- источник данных (например 'tpch.tiny')
);


-- =====================================================================
-- LNK_LINEITEM / SAT_LINEITEM / SAT_LINEITEM_STATUS   СТРОКА ЗАКАЗА (СВЯЗЬ ЗАКАЗ-ПАРТИЯ-ПОСТАВЩИК)
-- =====================================================================

-- LNK_LINEITEM — транзакционный link “строка заказа”:
-- связывает ORDER и PARTSUPP, плюс linenumber для уникальности строки в заказе.
CREATE TABLE lnk_lineitem (
  hlk_lineitem         VARBINARY,               	-- HLK: md5(hex(hk_order)|hex(hlk_part_supplier)|l_linenumber)
  hk_order             VARBINARY,               	-- ссылка на HUB_ORDER
  hlk_part_supplier    VARBINARY,               	-- ссылка на LNK_PART_SUPPLIER 
  l_linenumber         INTEGER,                 	-- номер строки в заказе (часть натурального ключа строки)
  load_dts            TIMESTAMP(3) WITH TIME ZONE,	-- техническая дата загрузки HLK
  record_source       VARCHAR						-- источник данных (например 'tpch.tiny')
);

-- SAT_LINEITEM — метрики строки заказа
CREATE TABLE sat_lineitem (
  hlk_lineitem         VARBINARY,               	-- ссылка на LNK_LINEITEM
  load_dts             TIMESTAMP(3) WITH TIME ZONE,	-- момент загрузки версии SAT
  hashdiff             VARBINARY,               	-- md5(количества/цены/скидки/даты/инструкции/комментарий)
  l_quantity           DOUBLE,						-- колличество
  l_extendedprice      DOUBLE,						-- сумма по строке
  l_discount           DOUBLE,						-- скидка
  l_tax                DOUBLE,						-- налог
  l_returnflag         VARCHAR,                 	-- флаг возврата
  l_shipdate           DATE,						-- дата доставки
  l_commitdate         DATE,						-- дата подтверждения
  l_receiptdate        DATE,						-- дата оплаты
  l_shipinstruct       VARCHAR,						-- комментарий по доставке
  l_shipmode           VARCHAR,						-- тип доставки
  l_comment            VARCHAR,						-- комментарий
  record_source        VARCHAR,						-- источник данных
  valid_from           TIMESTAMP(3) WITH TIME ZONE,	-- начало актуальности
  valid_to             TIMESTAMP(3) WITH TIME ZONE,	-- конец актуальности (открытая версия = 9999-12-31)
  is_deleted           BOOLEAN						-- признак логического удаления
);

-- SAT_LINEITEM_STATUS — статус строки заказа (часто меняется)
CREATE TABLE sat_lineitem_status (
  hlk_lineitem         VARBINARY,               	-- ссылка на LNK_LINEITEM
  load_dts             TIMESTAMP(3) WITH TIME ZONE,	-- момент загрузки версии SAT
  hashdiff             VARBINARY,               	-- md5(l_linestatus)
  l_linestatus         VARCHAR,						-- статус строки заказа
  record_source        VARCHAR,						-- источник данных
  valid_from           TIMESTAMP(3) WITH TIME ZONE,	-- начало актуальности
  valid_to             TIMESTAMP(3) WITH TIME ZONE,	-- конец актуальности (открытая версия = 9999-12-31)
  is_deleted           BOOLEAN						-- признак логического удаления
);

