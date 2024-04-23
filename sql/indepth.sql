----- Indepth -----
-- How much profit is earned from the user?
-- Service usage situation by customers
SELECT
  "hauct_endcourse_data"."dim_order"."customer_id" AS "customer_id",
  SUM(
    "hauct_endcourse_data"."dim_order"."product_number"
  ) AS "sum",
  SUM("hauct_endcourse_data"."dim_order"."revenue") AS "sum_2",
  SUM("hauct_endcourse_data"."dim_order"."cost") AS "sum_3",
  SUM("hauct_endcourse_data"."dim_order"."profit") AS "sum_4"
FROM
  "hauct_endcourse_data"."dim_order"
GROUP BY
  "hauct_endcourse_data"."dim_order"."customer_id"
ORDER BY
  "sum_4" DESC,
  "hauct_endcourse_data"."dim_order"."customer_id" ASC

-- How is the profit distributed?
-- Profit distribution by categories
SELECT
  "Dim Product - Product"."category" AS "Dim Product - Product__category",
  "Dim Product - Product"."sub_category" AS "Dim Product - Product__sub_category",
  SUM("hauct_endcourse_data"."dim_order"."profit") AS "sum"
FROM
  "hauct_endcourse_data"."dim_order"
 
LEFT JOIN "hauct_endcourse_data"."dim_product" AS "Dim Product - Product" ON "hauct_endcourse_data"."dim_order"."product_id" = "Dim Product - Product"."product_id"
GROUP BY
  "Dim Product - Product"."category",
  "Dim Product - Product"."sub_category"
ORDER BY
  "Dim Product - Product"."category" ASC,
  "Dim Product - Product"."sub_category" ASC

-- Profit distribution by age
SELECT
  "source"."age_type" AS "age_type",
  SUM("source"."profit") AS "sum"
FROM
  (
    SELECT
      "hauct_endcourse_data"."dim_order"."customer_id" AS "customer_id",
      "hauct_endcourse_data"."dim_order"."profit" AS "profit",
      CAST(
        extract(
          year
          from
            AGE(
              DATE_TRUNC('day', NOW()),
              DATE_TRUNC('day', "Dim Customer - Customer"."birth_date")
            )
        ) AS integer
      ) AS "age",
      CASE
        WHEN CAST(
          extract(
            year
            from
              AGE(
                DATE_TRUNC('day', NOW()),
                DATE_TRUNC('day', "Dim Customer - Customer"."birth_date")
              )
          ) AS integer
        ) < 18 THEN 'Under 18'
        WHEN CAST(
          extract(
            year
            from
              AGE(
                DATE_TRUNC('day', NOW()),
                DATE_TRUNC('day', "Dim Customer - Customer"."birth_date")
              )
          ) AS integer
        ) BETWEEN 18
   AND 24 THEN '18-24'
        WHEN CAST(
          extract(
            year
            from
              AGE(
                DATE_TRUNC('day', NOW()),
                DATE_TRUNC('day', "Dim Customer - Customer"."birth_date")
              )
          ) AS integer
        ) BETWEEN 25 AND 34 THEN '25-34'
        WHEN CAST(
          extract(
            year
            from
              AGE(
                DATE_TRUNC('day', NOW()),
                DATE_TRUNC('day', "Dim Customer - Customer"."birth_date")
              )
          ) AS integer
        ) BETWEEN 35 AND 44 THEN '35-44'
        WHEN CAST(
          extract(
            year
            from
              AGE(
                DATE_TRUNC('day', NOW()),
                DATE_TRUNC('day', "Dim Customer - Customer"."birth_date")
              )
          ) AS integer
        ) BETWEEN 45 AND 54 THEN '45-54'
        WHEN CAST(
          extract(
            year
            from
              AGE(
                DATE_TRUNC('day', NOW()),
                DATE_TRUNC('day', "Dim Customer - Customer"."birth_date")
              )
          ) AS integer
        ) BETWEEN 55 AND 64 THEN '55-64'
        ELSE '65 and over'
      END AS "age_type",
      "Dim Customer - Customer"."birth_date" AS "Dim Customer - Customer__birth_date",
      "Dim Customer - Customer"."customer_id" AS "Dim Customer - Customer__customer_id"
    FROM
      "hauct_endcourse_data"."dim_order"
     
LEFT JOIN "hauct_endcourse_data"."dim_customer" AS "Dim Customer - Customer" ON "hauct_endcourse_data"."dim_order"."customer_id" = "Dim Customer - Customer"."customer_id"
  ) AS "source"
GROUP BY
  "source"."age_type"
ORDER BY
  "source"."age_type" ASC

-- Profit distribution by provinces
SELECT
  "Dim Address - Address"."province" AS "Dim Address - Address__province",
  SUM("hauct_endcourse_data"."dim_order"."profit") AS "sum"
FROM
  "hauct_endcourse_data"."dim_order"
 
LEFT JOIN "hauct_endcourse_data"."dim_address" AS "Dim Address - Address" ON "hauct_endcourse_data"."dim_order"."address_id" = "Dim Address - Address"."address_id"
GROUP BY
  "Dim Address - Address"."province"
ORDER BY
  "sum" DESC,
  "Dim Address - Address"."province" ASC