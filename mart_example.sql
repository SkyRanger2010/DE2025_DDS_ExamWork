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
