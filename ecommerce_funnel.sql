CREATE OR REPLACE TABLE `your_project.your_dataset.your_table`
PARTITION BY event_date AS (

  SELECT

    event_date,
    event_name,
    utm_source,
    utm_medium,
    utm_campaign,
    geo_country,
    CASE
      WHEN geo_region = '(not set)' THEN 'Other'
      ELSE geo_region
    END AS geo_region,
    device_category,
    device_mobile_brand_name,
    COUNT(*) AS event_count

  FROM
    `your_project.your_dataset.your_table`

  WHERE
    event_name IN ('session_start', 'view_item', 'select_item', 'add_to_cart', 'begin_checkout', 'add_payment_info', 'purchase')
    
  GROUP BY ALL
   
)
