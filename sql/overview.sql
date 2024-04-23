----- Overview -----
-- Number of orders
SELECT
  COUNT(*) AS "count"
FROM
  "hauct_endcourse_data"."dim_order"

-- Number of customers
SELECT
  COUNT(*) AS "count"
FROM
  "hauct_endcourse_data"."dim_customer"

-- Revenue
SELECT
  SUM("hauct_endcourse_data"."dim_order"."revenue") AS "sum"
FROM
  "hauct_endcourse_data"."dim_order"

-- Cost
SELECT
  SUM("hauct_endcourse_data"."dim_order"."cost") AS "sum"
FROM
  "hauct_endcourse_data"."dim_order"

-- Profit
SELECT
  SUM("hauct_endcourse_data"."dim_order"."profit") AS "sum"
FROM
  "hauct_endcourse_data"."dim_order"

----- How is business situation? -----
-- Business situation - Days
SELECT
  "hauct_endcourse_data"."dim_order"."order_date" AS "order_date",
  SUM("hauct_endcourse_data"."dim_order"."revenue") AS "sum",
  SUM("hauct_endcourse_data"."dim_order"."cost") AS "sum_2",
  SUM("hauct_endcourse_data"."dim_order"."profit") AS "sum_3"
FROM
  "hauct_endcourse_data"."dim_order"
GROUP BY
  "hauct_endcourse_data"."dim_order"."order_date"
ORDER BY
  "hauct_endcourse_data"."dim_order"."order_date" ASC

-- Business situation - Weeks
SELECT
  (
    DATE_TRUNC(
      'week',
      (
        "hauct_endcourse_data"."dim_order"."order_date" + INTERVAL '1 day'
      )
    ) + INTERVAL '-1 day'
  ) AS "order_date",
  SUM("hauct_endcourse_data"."dim_order"."revenue") AS "sum",
  SUM("hauct_endcourse_data"."dim_order"."cost") AS "sum_2",
  SUM("hauct_endcourse_data"."dim_order"."profit") AS "sum_3"
FROM
  "hauct_endcourse_data"."dim_order"
GROUP BY
  (
    DATE_TRUNC(
      'week',
      (
        "hauct_endcourse_data"."dim_order"."order_date" + INTERVAL '1 day'
      )
    ) + INTERVAL '-1 day'
  )
ORDER BY
  (
    DATE_TRUNC(
      'week',
      (
        "hauct_endcourse_data"."dim_order"."order_date" + INTERVAL '1 day'
      )
    ) + INTERVAL '-1 day'
  ) ASC

-- The quantity of orders - Days
SELECT
  "hauct_endcourse_data"."dim_order"."order_date" AS "order_date",
  COUNT(*) AS "count"
FROM
  "hauct_endcourse_data"."dim_order"
GROUP BY
  "hauct_endcourse_data"."dim_order"."order_date"
ORDER BY
  "hauct_endcourse_data"."dim_order"."order_date" ASC

-- The quantity of orders - Weeks
SELECT
  (
    DATE_TRUNC(
      'week',
      (
        "hauct_endcourse_data"."dim_order"."order_date" + INTERVAL '1 day'
      )
    ) + INTERVAL '-1 day'
  ) AS "order_date",
  COUNT(*) AS "count"
FROM
  "hauct_endcourse_data"."dim_order"
GROUP BY
  (
    DATE_TRUNC(
      'week',
      (
        "hauct_endcourse_data"."dim_order"."order_date" + INTERVAL '1 day'
      )
    ) + INTERVAL '-1 day'
  )
ORDER BY
  (
    DATE_TRUNC(
      'week',
      (
        "hauct_endcourse_data"."dim_order"."order_date" + INTERVAL '1 day'
      )
    ) + INTERVAL '-1 day'
  ) ASC