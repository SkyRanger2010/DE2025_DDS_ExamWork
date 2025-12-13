-- =============================================================================================
--                УСОВЕРШЕНСТВОВАННЫЙ СКРИПТ
-- 
-- Один раз материализуем “срез” источника src_* — и все остальные шаги работают только с ним.
-- При load_date = NULL автоматически грузится полный TPC-H tiny.
-- При заданной load_date грузится только “подграф” сущностей, связанных с заказами этой даты.
--
-- ============================================================================================

USE memory.data_vault;

DROP TABLE IF EXISTS dv_run_control;

CREATE TABLE dv_run_control (
  run_id     BIGINT,
  load_ts    TIMESTAMP(3) WITH TIME ZONE,
  load_date  DATE,                          -- параметр (может быть NULL)
  open_end   TIMESTAMP(3) WITH TIME ZONE,
  src_name   VARCHAR
);

-- Один активный прогон
INSERT INTO dv_run_control (run_id, load_ts, load_date, open_end, src_name)
VALUES (
  1,
  current_timestamp,
  TIMESTAMP '1998-04-22 00:00:00 UTC',     -- если дата -> грузим все заказы за эту дату и все данные связанные с этими заказами
  --NULL,								   -- если NULL -> грузим все заказы
  TIMESTAMP '9999-12-31 00:00:00 UTC',
  'tpch.tiny'
);

-- Создаем срезы таблиц источника в зависимости от параметра load_date

DROP TABLE IF EXISTS src_orders;

CREATE TABLE src_orders AS
SELECT o.*
FROM tpch.tiny.orders o
CROSS JOIN dv_run_control r
WHERE r.run_id = 1
  AND (r.load_date IS NULL OR o.orderdate = r.load_date);

DROP TABLE IF EXISTS src_lineitem;

CREATE TABLE src_lineitem AS
SELECT li.*
FROM tpch.tiny.lineitem li
WHERE EXISTS (
  SELECT 1 FROM src_orders o
  WHERE o.orderkey = li.orderkey
);

DROP TABLE IF EXISTS src_customer;

CREATE TABLE src_customer AS
SELECT c.*
FROM tpch.tiny.customer c
WHERE EXISTS (
  SELECT 1 FROM src_orders o
  WHERE o.custkey = c.custkey
);

DROP TABLE IF EXISTS src_supplier;

CREATE TABLE src_supplier AS
SELECT s.*
FROM tpch.tiny.supplier s
WHERE EXISTS (
  SELECT 1 FROM src_lineitem li
  WHERE li.suppkey = s.suppkey
);

DROP TABLE IF EXISTS src_part;

CREATE TABLE src_part AS
SELECT p.*
FROM tpch.tiny.part p
WHERE EXISTS (
  SELECT 1 FROM src_lineitem li
  WHERE li.partkey = p.partkey
);

DROP TABLE IF EXISTS src_partsupp;

CREATE TABLE src_partsupp AS
SELECT ps.*
FROM tpch.tiny.partsupp ps
WHERE EXISTS (
  SELECT 1
  FROM src_lineitem li
  WHERE li.partkey = ps.partkey
    AND li.suppkey = ps.suppkey
);

DROP TABLE IF EXISTS src_nation;

CREATE TABLE src_nation AS
SELECT n.*
FROM tpch.tiny.nation n
WHERE EXISTS (SELECT 1 FROM src_customer c WHERE c.nationkey = n.nationkey)
   OR EXISTS (SELECT 1 FROM src_supplier s WHERE s.nationkey = n.nationkey);

DROP TABLE IF EXISTS src_region;

CREATE TABLE src_region AS
SELECT r.*
FROM tpch.tiny.region r
WHERE EXISTS (SELECT 1 FROM src_nation n WHERE n.regionkey = r.regionkey);

-- ========================================================================
-- Создаем вспомогательные представления на основе таблиц срезов
-- ========================================================================

-- REGION
CREATE OR REPLACE VIEW v_src_region AS
SELECT
  md5(to_utf8(cast(regionkey AS varchar))) AS hk_region,
  cast(regionkey AS bigint)                AS region_key,
  name                                     AS r_name,
  comment                                  AS r_comment,
  md5(to_utf8(concat_ws('|', coalesce(name,''), coalesce(comment,'')))) AS hashdiff
FROM src_region;

-- NATION
CREATE OR REPLACE VIEW v_src_nation AS
SELECT
  md5(to_utf8(cast(nationkey AS varchar))) AS hk_nation,
  cast(nationkey AS bigint)                AS nation_key,
  md5(to_utf8(cast(regionkey AS varchar))) AS hk_region,
  name                                     AS n_name,
  comment                                  AS n_comment,
  md5(to_utf8(concat_ws('|', coalesce(name,''), coalesce(comment,'')))) AS hashdiff
FROM src_nation;

-- CUSTOMER
CREATE OR REPLACE VIEW v_src_customer AS
SELECT
  md5(to_utf8(cast(custkey AS varchar)))    AS hk_customer,
  cast(custkey AS bigint)                   AS cust_key,
  md5(to_utf8(cast(nationkey AS varchar)))  AS hk_nation,
  name                                      AS c_name,
  address                                   AS c_address,
  phone                                     AS c_phone,
  mktsegment                                AS c_mktsegment,
  comment                                   AS c_comment,
  acctbal                                   AS c_acctbal,
  md5(to_utf8(concat_ws('|',
    coalesce(name,''), coalesce(address,''), coalesce(phone,''),
    coalesce(mktsegment,''), coalesce(comment,'')
  ))) AS hashdiff_main,
  md5(to_utf8(coalesce(cast(acctbal AS varchar),''))) AS hashdiff_balance
FROM src_customer;

-- SUPPLIER
CREATE OR REPLACE VIEW v_src_supplier AS
SELECT
  md5(to_utf8(cast(suppkey AS varchar)))    AS hk_supplier,
  cast(suppkey AS bigint)                   AS supp_key,
  md5(to_utf8(cast(nationkey AS varchar)))  AS hk_nation,
  name                                      AS s_name,
  address                                   AS s_address,
  phone                                     AS s_phone,
  comment                                   AS s_comment,
  acctbal                                   AS s_acctbal,
  md5(to_utf8(concat_ws('|',
    coalesce(name,''), coalesce(address,''), coalesce(phone,''), coalesce(comment,'')
  ))) AS hashdiff_main,
  md5(to_utf8(coalesce(cast(acctbal AS varchar),''))) AS hashdiff_balance
FROM src_supplier;

-- PART
CREATE OR REPLACE VIEW v_src_part AS
SELECT
  md5(to_utf8(cast(partkey AS varchar))) AS hk_part,
  cast(partkey AS bigint)                AS part_key,
  name                                   AS p_name,
  mfgr                                   AS p_mfgr,
  brand                                  AS p_brand,
  type                                   AS p_type,
  cast(size AS integer)                  AS p_size,
  container                              AS p_container,
  retailprice                            AS p_retailprice,
  comment                                AS p_comment,
  md5(to_utf8(concat_ws('|',
    coalesce(name,''), coalesce(mfgr,''), coalesce(brand,''), coalesce(type,''),
    coalesce(cast(size AS varchar),''), coalesce(container,''),
    coalesce(cast(retailprice AS varchar),''), coalesce(comment,'')
  ))) AS hashdiff
FROM src_part;

-- ORDER
CREATE OR REPLACE VIEW v_src_order AS
SELECT
  md5(to_utf8(cast(orderkey AS varchar))) AS hk_order,
  cast(orderkey AS bigint)                AS order_key,
  md5(to_utf8(cast(custkey AS varchar)))  AS hk_customer,
  totalprice                              AS o_totalprice,
  orderdate                               AS o_orderdate,
  orderpriority                           AS o_orderpriority,
  clerk                                   AS o_clerk,
  cast(shippriority AS integer)           AS o_shippriority,
  comment                                 AS o_comment,
  orderstatus                             AS o_orderstatus,
  md5(to_utf8(concat_ws('|',
    coalesce(cast(totalprice AS varchar),''), coalesce(cast(orderdate AS varchar),''), coalesce(orderpriority,''),
    coalesce(clerk,''), coalesce(cast(shippriority AS varchar),''), coalesce(comment,'')
  ))) AS hashdiff_main,
  md5(to_utf8(coalesce(orderstatus,'')))  AS hashdiff_status
FROM src_orders;

-- PARTSUPP 
CREATE OR REPLACE VIEW v_src_partsupp AS
SELECT
  md5(to_utf8(cast(partkey AS varchar))) AS hk_part,
  md5(to_utf8(cast(suppkey AS varchar))) AS hk_supplier,

  md5(to_utf8(concat(
    to_hex(md5(to_utf8(cast(partkey AS varchar)))), '|',
    to_hex(md5(to_utf8(cast(suppkey AS varchar))))
  ))) AS hlk_part_supplier,

  cast(availqty AS integer)              AS ps_availqty,
  supplycost                             AS ps_supplycost,
  comment                                AS ps_comment,
  md5(to_utf8(concat_ws('|',
    coalesce(cast(availqty AS varchar),''), coalesce(cast(supplycost AS varchar),''), coalesce(comment,'')
  ))) AS hashdiff
FROM src_partsupp;

-- LINEITEM 
CREATE OR REPLACE VIEW v_src_lineitem AS
SELECT
  md5(to_utf8(cast(orderkey AS varchar))) AS hk_order,

  md5(to_utf8(concat(
    to_hex(md5(to_utf8(cast(partkey AS varchar)))), '|',
    to_hex(md5(to_utf8(cast(suppkey AS varchar))))
  ))) AS hlk_part_supplier,

  cast(linenumber AS integer)            AS l_linenumber,

  md5(to_utf8(concat(
    to_hex(md5(to_utf8(cast(orderkey AS varchar)))), '|',
    to_hex(md5(to_utf8(concat(
      to_hex(md5(to_utf8(cast(partkey AS varchar)))), '|',
      to_hex(md5(to_utf8(cast(suppkey AS varchar))))
    )))), '|',
    cast(linenumber AS varchar)
  ))) AS hlk_lineitem,

  quantity                               AS l_quantity,
  extendedprice                          AS l_extendedprice,
  discount                               AS l_discount,
  tax                                    AS l_tax,
  returnflag                             AS l_returnflag,
  linestatus                             AS l_linestatus,
  shipdate                               AS l_shipdate,
  commitdate                             AS l_commitdate,
  receiptdate                            AS l_receiptdate,
  shipinstruct                           AS l_shipinstruct,
  shipmode                               AS l_shipmode,
  comment                                AS l_comment,

  md5(to_utf8(concat_ws('|',
    coalesce(cast(quantity AS varchar),''), coalesce(cast(extendedprice AS varchar),''), coalesce(cast(discount AS varchar),''), coalesce(cast(tax AS varchar),''),
    coalesce(returnflag,''), coalesce(cast(shipdate AS varchar),''), coalesce(cast(commitdate AS varchar),''), coalesce(cast(receiptdate AS varchar),''),
    coalesce(shipinstruct,''), coalesce(shipmode,''), coalesce(comment,'')
  ))) AS hashdiff_main,

  md5(to_utf8(coalesce(linestatus,'')))  AS hashdiff_status
FROM src_lineitem;

-- ====================================================================
-- Заполняем хабы данными из источника (только вставка новых)
-- ====================================================================

INSERT INTO hub_region (hk_region, region_key, load_dts, record_source)
SELECT s.hk_region, s.region_key, current_timestamp, r.src_name
FROM v_src_region s
CROSS JOIN dv_run_control r
LEFT JOIN hub_region h ON h.hk_region = s.hk_region
WHERE r.run_id = 1
  AND h.hk_region IS NULL;

INSERT INTO hub_nation (hk_nation, nation_key, load_dts, record_source)
SELECT s.hk_nation, s.nation_key, current_timestamp, r.src_name
FROM v_src_nation s
CROSS JOIN dv_run_control r
LEFT JOIN hub_nation h ON h.hk_nation = s.hk_nation
WHERE r.run_id = 1
  AND h.hk_nation IS NULL;

INSERT INTO hub_customer (hk_customer, cust_key, load_dts, record_source)
SELECT s.hk_customer, s.cust_key, current_timestamp, r.src_name
FROM v_src_customer s
CROSS JOIN dv_run_control r
LEFT JOIN hub_customer h ON h.hk_customer = s.hk_customer
WHERE r.run_id = 1
  AND h.hk_customer IS NULL;

INSERT INTO hub_supplier (hk_supplier, supp_key, load_dts, record_source)
SELECT s.hk_supplier, s.supp_key, current_timestamp, r.src_name
FROM v_src_supplier s
CROSS JOIN dv_run_control r
LEFT JOIN hub_supplier h ON h.hk_supplier = s.hk_supplier
WHERE r.run_id = 1
  AND h.hk_supplier IS NULL;

INSERT INTO hub_part (hk_part, part_key, load_dts, record_source)
SELECT s.hk_part, s.part_key, current_timestamp, r.src_name
FROM v_src_part s
CROSS JOIN dv_run_control r
LEFT JOIN hub_part h ON h.hk_part = s.hk_part
WHERE r.run_id = 1
  AND h.hk_part IS NULL;

INSERT INTO hub_order (hk_order, order_key, load_dts, record_source)
SELECT s.hk_order, s.order_key, current_timestamp, r.src_name
FROM v_src_order s
CROSS JOIN dv_run_control r
LEFT JOIN hub_order h ON h.hk_order = s.hk_order
WHERE r.run_id = 1
  AND h.hk_order IS NULL;

-- ==================================================================
-- Заполняем линки данными из источника (только вставка новых)
-- ==================================================================


INSERT INTO lnk_nation_region (hlk_nation_region, hk_nation, hk_region, load_dts, record_source)
SELECT
  md5(to_utf8(concat(to_hex(s.hk_nation), '|', to_hex(s.hk_region)))),
  s.hk_nation, s.hk_region, current_timestamp, r.src_name
FROM v_src_nation s
CROSS JOIN dv_run_control r
LEFT JOIN lnk_nation_region l
  ON l.hlk_nation_region = md5(to_utf8(concat(to_hex(s.hk_nation), '|', to_hex(s.hk_region))))
WHERE r.run_id = 1
  AND l.hlk_nation_region IS NULL;

INSERT INTO lnk_customer_nation (hlk_customer_nation, hk_customer, hk_nation, load_dts, record_source)
SELECT
  md5(to_utf8(concat(to_hex(s.hk_customer), '|', to_hex(s.hk_nation)))),
  s.hk_customer, s.hk_nation, current_timestamp, r.src_name
FROM v_src_customer s
CROSS JOIN dv_run_control r
LEFT JOIN lnk_customer_nation l
  ON l.hlk_customer_nation = md5(to_utf8(concat(to_hex(s.hk_customer), '|', to_hex(s.hk_nation))))
WHERE r.run_id = 1
  AND l.hlk_customer_nation IS NULL;

INSERT INTO lnk_supplier_nation (hlk_supplier_nation, hk_supplier, hk_nation, load_dts, record_source)
SELECT
  md5(to_utf8(concat(to_hex(s.hk_supplier), '|', to_hex(s.hk_nation)))),
  s.hk_supplier, s.hk_nation, current_timestamp, r.src_name
FROM v_src_supplier s
CROSS JOIN dv_run_control r
LEFT JOIN lnk_supplier_nation l
  ON l.hlk_supplier_nation = md5(to_utf8(concat(to_hex(s.hk_supplier), '|', to_hex(s.hk_nation))))
WHERE r.run_id = 1
  AND l.hlk_supplier_nation IS NULL;

INSERT INTO lnk_order_customer (hlk_order_customer, hk_order, hk_customer, load_dts, record_source)
SELECT
  md5(to_utf8(concat(to_hex(s.hk_order), '|', to_hex(s.hk_customer)))),
  s.hk_order, s.hk_customer, current_timestamp, r.src_name
FROM v_src_order s
CROSS JOIN dv_run_control r
LEFT JOIN lnk_order_customer l
  ON l.hlk_order_customer = md5(to_utf8(concat(to_hex(s.hk_order), '|', to_hex(s.hk_customer))))
WHERE r.run_id = 1
  AND l.hlk_order_customer IS NULL;

INSERT INTO lnk_part_supplier (hlk_part_supplier, hk_part, hk_supplier, load_dts, record_source)
SELECT s.hlk_part_supplier, s.hk_part, s.hk_supplier, current_timestamp, r.src_name
FROM v_src_partsupp s
CROSS JOIN dv_run_control r
LEFT JOIN lnk_part_supplier l ON l.hlk_part_supplier = s.hlk_part_supplier
WHERE r.run_id = 1
  AND l.hlk_part_supplier IS NULL;

INSERT INTO lnk_lineitem (hlk_lineitem, hk_order, hlk_part_supplier, l_linenumber, load_dts, record_source)
SELECT s.hlk_lineitem, s.hk_order, s.hlk_part_supplier, s.l_linenumber, current_timestamp, r.src_name
FROM v_src_lineitem s
CROSS JOIN dv_run_control r
LEFT JOIN lnk_lineitem l ON l.hlk_lineitem = s.hlk_lineitem
WHERE r.run_id = 1
  AND l.hlk_lineitem IS NULL;

-- ==========================================================================================
-- Заполняем спутники данными из источника 
-- 
-- Так как UPDATE в коннекторе memory запрещен - valid_to всегда остается 9999-12-31, при 
-- новая версия данных сравнивается с версией с самым поздним valid_from и если есть различия
-- просто добавляется с новым значением valid_from
-- ==========================================================================================


-- ==========================================================
-- SAT_REGION (ключ: hk_region)
-- ==========================================================
INSERT INTO sat_region
(hk_region, load_dts, hashdiff, r_name, r_comment, record_source, valid_from, valid_to, is_deleted)
SELECT
  s.hk_region,
  r.load_ts,
  s.hashdiff,
  s.r_name,
  s.r_comment,
  r.src_name,
  r.load_ts,
  r.open_end,
  false
FROM v_src_region s
CROSS JOIN dv_run_control r
LEFT JOIN (
  SELECT hk_region, max(valid_from) AS max_vf
  FROM sat_region
  GROUP BY hk_region
) last
  ON last.hk_region = s.hk_region
LEFT JOIN sat_region cur
  ON cur.hk_region  = s.hk_region
 AND cur.valid_from = last.max_vf
WHERE r.run_id = 1
  AND (cur.hk_region IS NULL OR cur.hashdiff <> s.hashdiff);


-- ==========================================================
-- SAT_NATION (ключ: hk_nation)
-- ==========================================================
INSERT INTO sat_nation
(hk_nation, load_dts, hashdiff, n_name, n_comment, record_source, valid_from, valid_to, is_deleted)
SELECT
  s.hk_nation,
  r.load_ts,
  s.hashdiff,
  s.n_name,
  s.n_comment,
  r.src_name,
  r.load_ts,
  r.open_end,
  false
FROM v_src_nation s
CROSS JOIN dv_run_control r
LEFT JOIN (
  SELECT hk_nation, max(valid_from) AS max_vf
  FROM sat_nation
  GROUP BY hk_nation
) last
  ON last.hk_nation = s.hk_nation
LEFT JOIN sat_nation cur
  ON cur.hk_nation  = s.hk_nation
 AND cur.valid_from = last.max_vf
WHERE r.run_id = 1
  AND (cur.hk_nation IS NULL OR cur.hashdiff <> s.hashdiff);


-- ==========================================================
-- SAT_CUSTOMER (ключ: hk_customer, hashdiff: hashdiff_main)
-- ==========================================================
INSERT INTO sat_customer
(hk_customer, load_dts, hashdiff, c_name, c_address, c_phone, c_mktsegment, c_comment,
 record_source, valid_from, valid_to, is_deleted)
SELECT
  s.hk_customer,
  r.load_ts,
  s.hashdiff_main,
  s.c_name,
  s.c_address,
  s.c_phone,
  s.c_mktsegment,
  s.c_comment,
  r.src_name,
  r.load_ts,
  r.open_end,
  false
FROM v_src_customer s
CROSS JOIN dv_run_control r
LEFT JOIN (
  SELECT hk_customer, max(valid_from) AS max_vf
  FROM sat_customer
  GROUP BY hk_customer
) last
  ON last.hk_customer = s.hk_customer
LEFT JOIN sat_customer cur
  ON cur.hk_customer = s.hk_customer
 AND cur.valid_from  = last.max_vf
WHERE r.run_id = 1
  AND (cur.hk_customer IS NULL OR cur.hashdiff <> s.hashdiff_main);


-- ==========================================================
-- SAT_CUSTOMER_BALANCE (ключ: hk_customer, hashdiff: hashdiff_balance)
-- ==========================================================
INSERT INTO sat_customer_balance
(hk_customer, load_dts, hashdiff, c_acctbal, record_source, valid_from, valid_to, is_deleted)
SELECT
  s.hk_customer,
  r.load_ts,
  s.hashdiff_balance,
  s.c_acctbal,
  r.src_name,
  r.load_ts,
  r.open_end,
  false
FROM v_src_customer s
CROSS JOIN dv_run_control r
LEFT JOIN (
  SELECT hk_customer, max(valid_from) AS max_vf
  FROM sat_customer_balance
  GROUP BY hk_customer
) last
  ON last.hk_customer = s.hk_customer
LEFT JOIN sat_customer_balance cur
  ON cur.hk_customer = s.hk_customer
 AND cur.valid_from  = last.max_vf
WHERE r.run_id = 1
  AND (cur.hk_customer IS NULL OR cur.hashdiff <> s.hashdiff_balance);


-- ==========================================================
-- SAT_SUPPLIER (ключ: hk_supplier, hashdiff: hashdiff_main)
-- ==========================================================
INSERT INTO sat_supplier
(hk_supplier, load_dts, hashdiff, s_name, s_address, s_phone, s_comment,
 record_source, valid_from, valid_to, is_deleted)
SELECT
  s.hk_supplier,
  r.load_ts,
  s.hashdiff_main,
  s.s_name,
  s.s_address,
  s.s_phone,
  s.s_comment,
  r.src_name,
  r.load_ts,
  r.open_end,
  false
FROM v_src_supplier s
CROSS JOIN dv_run_control r
LEFT JOIN (
  SELECT hk_supplier, max(valid_from) AS max_vf
  FROM sat_supplier
  GROUP BY hk_supplier
) last
  ON last.hk_supplier = s.hk_supplier
LEFT JOIN sat_supplier cur
  ON cur.hk_supplier = s.hk_supplier
 AND cur.valid_from  = last.max_vf
WHERE r.run_id = 1
  AND (cur.hk_supplier IS NULL OR cur.hashdiff <> s.hashdiff_main);


-- ==========================================================
-- SAT_SUPPLIER_BALANCE (ключ: hk_supplier, hashdiff: hashdiff_balance)
-- ==========================================================
INSERT INTO sat_supplier_balance
(hk_supplier, load_dts, hashdiff, s_acctbal, record_source, valid_from, valid_to, is_deleted)
SELECT
  s.hk_supplier,
  r.load_ts,
  s.hashdiff_balance,
  s.s_acctbal,
  r.src_name,
  r.load_ts,
  r.open_end,
  false
FROM v_src_supplier s
CROSS JOIN dv_run_control r
LEFT JOIN (
  SELECT hk_supplier, max(valid_from) AS max_vf
  FROM sat_supplier_balance
  GROUP BY hk_supplier
) last
  ON last.hk_supplier = s.hk_supplier
LEFT JOIN sat_supplier_balance cur
  ON cur.hk_supplier = s.hk_supplier
 AND cur.valid_from  = last.max_vf
WHERE r.run_id = 1
  AND (cur.hk_supplier IS NULL OR cur.hashdiff <> s.hashdiff_balance);


-- ==========================================================
-- SAT_PART (ключ: hk_part)
-- ==========================================================
INSERT INTO sat_part
(hk_part, load_dts, hashdiff, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment,
 record_source, valid_from, valid_to, is_deleted)
SELECT
  s.hk_part,
  r.load_ts,
  s.hashdiff,
  s.p_name,
  s.p_mfgr,
  s.p_brand,
  s.p_type,
  s.p_size,
  s.p_container,
  s.p_retailprice,
  s.p_comment,
  r.src_name,
  r.load_ts,
  r.open_end,
  false
FROM v_src_part s
CROSS JOIN dv_run_control r
LEFT JOIN (
  SELECT hk_part, max(valid_from) AS max_vf
  FROM sat_part
  GROUP BY hk_part
) last
  ON last.hk_part = s.hk_part
LEFT JOIN sat_part cur
  ON cur.hk_part   = s.hk_part
 AND cur.valid_from = last.max_vf
WHERE r.run_id = 1
  AND (cur.hk_part IS NULL OR cur.hashdiff <> s.hashdiff);


-- ==========================================================
-- SAT_ORDER (ключ: hk_order, hashdiff: hashdiff_main)
-- ==========================================================
INSERT INTO sat_order
(hk_order, load_dts, hashdiff, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment,
 record_source, valid_from, valid_to, is_deleted)
SELECT
  s.hk_order,
  r.load_ts,
  s.hashdiff_main,
  s.o_totalprice,
  s.o_orderdate,
  s.o_orderpriority,
  s.o_clerk,
  s.o_shippriority,
  s.o_comment,
  r.src_name,
  r.load_ts,
  r.open_end,
  false
FROM v_src_order s
CROSS JOIN dv_run_control r
LEFT JOIN (
  SELECT hk_order, max(valid_from) AS max_vf
  FROM sat_order
  GROUP BY hk_order
) last
  ON last.hk_order = s.hk_order
LEFT JOIN sat_order cur
  ON cur.hk_order  = s.hk_order
 AND cur.valid_from = last.max_vf
WHERE r.run_id = 1
  AND (cur.hk_order IS NULL OR cur.hashdiff <> s.hashdiff_main);


-- ==========================================================
-- SAT_ORDER_STATUS (ключ: hk_order, hashdiff: hashdiff_status)
-- ==========================================================
INSERT INTO sat_order_status
(hk_order, load_dts, hashdiff, o_orderstatus, record_source, valid_from, valid_to, is_deleted)
SELECT
  s.hk_order,
  r.load_ts,
  s.hashdiff_status,
  s.o_orderstatus,
  r.src_name,
  r.load_ts,
  r.open_end,
  false
FROM v_src_order s
CROSS JOIN dv_run_control r
LEFT JOIN (
  SELECT hk_order, max(valid_from) AS max_vf
  FROM sat_order_status
  GROUP BY hk_order
) last
  ON last.hk_order = s.hk_order
LEFT JOIN sat_order_status cur
  ON cur.hk_order  = s.hk_order
 AND cur.valid_from = last.max_vf
WHERE r.run_id = 1
  AND (cur.hk_order IS NULL OR cur.hashdiff <> s.hashdiff_status);


-- ==========================================================
-- SAT_PART_SUPPLIER (ключ: hlk_part_supplier)
-- ==========================================================
INSERT INTO sat_part_supplier
(hlk_part_supplier, load_dts, hashdiff, ps_availqty, ps_supplycost, ps_comment,
 record_source, valid_from, valid_to, is_deleted)
SELECT
  s.hlk_part_supplier,
  r.load_ts,
  s.hashdiff,
  s.ps_availqty,
  s.ps_supplycost,
  s.ps_comment,
  r.src_name,
  r.load_ts,
  r.open_end,
  false
FROM v_src_partsupp s
CROSS JOIN dv_run_control r
LEFT JOIN (
  SELECT hlk_part_supplier, max(valid_from) AS max_vf
  FROM sat_part_supplier
  GROUP BY hlk_part_supplier
) last
  ON last.hlk_part_supplier = s.hlk_part_supplier
LEFT JOIN sat_part_supplier cur
  ON cur.hlk_part_supplier = s.hlk_part_supplier
 AND cur.valid_from        = last.max_vf
WHERE r.run_id = 1
  AND (cur.hlk_part_supplier IS NULL OR cur.hashdiff <> s.hashdiff);


-- ==========================================================
-- SAT_LINEITEM (ключ: hlk_lineitem, hashdiff: hashdiff_main)
-- ==========================================================
INSERT INTO sat_lineitem
(hlk_lineitem, load_dts, hashdiff,
 l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag,
 l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment,
 record_source, valid_from, valid_to, is_deleted)
SELECT
  s.hlk_lineitem,
  r.load_ts,
  s.hashdiff_main,
  s.l_quantity,
  s.l_extendedprice,
  s.l_discount,
  s.l_tax,
  s.l_returnflag,
  s.l_shipdate,
  s.l_commitdate,
  s.l_receiptdate,
  s.l_shipinstruct,
  s.l_shipmode,
  s.l_comment,
  r.src_name,
  r.load_ts,
  r.open_end,
  false
FROM v_src_lineitem s
CROSS JOIN dv_run_control r
LEFT JOIN (
  SELECT hlk_lineitem, max(valid_from) AS max_vf
  FROM sat_lineitem
  GROUP BY hlk_lineitem
) last
  ON last.hlk_lineitem = s.hlk_lineitem
LEFT JOIN sat_lineitem cur
  ON cur.hlk_lineitem = s.hlk_lineitem
 AND cur.valid_from   = last.max_vf
WHERE r.run_id = 1
  AND (cur.hlk_lineitem IS NULL OR cur.hashdiff <> s.hashdiff_main);


-- ==========================================================
-- SAT_LINEITEM_STATUS (ключ: hlk_lineitem, hashdiff: hashdiff_status)
-- ==========================================================
INSERT INTO sat_lineitem_status
(hlk_lineitem, load_dts, hashdiff, l_linestatus,
 record_source, valid_from, valid_to, is_deleted)
SELECT
  s.hlk_lineitem,
  r.load_ts,
  s.hashdiff_status,
  s.l_linestatus,
  r.src_name,
  r.load_ts,
  r.open_end,
  false
FROM v_src_lineitem s
CROSS JOIN dv_run_control r
LEFT JOIN (
  SELECT hlk_lineitem, max(valid_from) AS max_vf
  FROM sat_lineitem_status
  GROUP BY hlk_lineitem
) last
  ON last.hlk_lineitem = s.hlk_lineitem
LEFT JOIN sat_lineitem_status cur
  ON cur.hlk_lineitem = s.hlk_lineitem
 AND cur.valid_from   = last.max_vf
WHERE r.run_id = 1
  AND (cur.hlk_lineitem IS NULL OR cur.hashdiff <> s.hashdiff_status);