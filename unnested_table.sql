CREATE OR REPLACE TABLE `your_project.your_dataset.your_table`
PARTITION BY event_date AS (



WITH
        step1 AS (

SELECT
        event_date,
        event_timestamp,
        user_pseudo_id,
        (SELECT value.int_value    FROM UNNEST (event_params) WHERE key = 'ga_session_id')         AS ga_session_id,
        (SELECT value.int_value    FROM UNNEST (event_params) WHERE key = 'ga_session_number')     AS ga_session_number,
        (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'session_engaged')       AS session_engaged,
        (SELECT value.int_value    FROM UNNEST (event_params) WHERE key = 'engagement_time_msec')  AS engagement_time_msec,
        (SELECT value.int_value    FROM UNNEST (event_params) WHERE key = 'engaged_session_event') AS engaged_session_event,

        device.category                    AS device_category,
        CASE
          WHEN device.mobile_brand_name         = '<Other>' THEN 'Other'
          ELSE device.mobile_brand_name
        END AS device_mobile_brand_name,

        CASE
          WHEN device.mobile_model_name         = '<Other>' THEN 'Other'
          ELSE device.mobile_model_name
        END AS device_mobile_model_name,

        CASE
          WHEN device.mobile_marketing_name     = '<Other>' THEN 'Other'
          ELSE device.mobile_marketing_name
        END AS device_mobile_marketing_name,

        CASE
          WHEN device.operating_system          = '<Other>' THEN 'Other'
          ELSE device.operating_system
        END AS device_operating_system,

        CASE
          WHEN device.web_info.browser          = '<Other>' THEN 'Other'
          ELSE device.web_info.browser
        END AS device_web_info_browser,

        CASE
          WHEN device.web_info.browser_version  = '<Other>' THEN 'Other'
          ELSE device.web_info.browser_version
        END AS device_web_info_browser_version,


        geo.continent                      AS geo_continent,
        geo.sub_continent                  AS geo_sub_content,
        geo.country                        AS geo_country,
        geo.region                         AS geo_region,
        geo.city                           AS geo_city,
     


        CASE 
        WHEN (SELECT value.int_value    FROM UNNEST(event_params) WHERE event_name = 'page_view' AND key = 'entrances') = 1 
        THEN (SELECT value.string_value FROM UNNEST(event_params) WHERE event_name = 'page_view' AND KEY = 'page_location') 
        END  AS landing_page,

        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location')       AS page_location,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer')       AS page_referrer,


         

        CASE
          WHEN (SELECT value.string_value FROM UNNEST(event_params)  WHERE key = 'source')      IN ('<Other>', '(data deleted)')  THEN 'Other'
          WHEN (SELECT value.string_value FROM UNNEST(event_params)  WHERE key = 'source')      IS NULL                           THEN 'Other'
          ELSE (SELECT value.string_value FROM UNNEST(event_params)  WHERE key = 'source') 
        END   AS utm_source,

        CASE
          WHEN (SELECT value.string_value FROM UNNEST(event_params)  WHERE key = 'medium')      IN ('<Other>', '(data deleted)')  THEN 'Other'
          WHEN (SELECT value.string_value FROM UNNEST(event_params)  WHERE key = 'medium')      IS NULL                           THEN 'Other'
          ELSE (SELECT value.string_value FROM UNNEST(event_params)  WHERE key = 'medium') 
        END   AS utm_medium,


        CASE
          WHEN (SELECT value.string_value FROM UNNEST(event_params)  WHERE key = 'campaign')    IN ('<Other>', '(data deleted)')  THEN 'Other'
          WHEN (SELECT value.string_value FROM UNNEST(event_params)  WHERE key = 'campaign')    IS NULL                           THEN 'Other'
          ELSE (SELECT value.string_value FROM UNNEST(event_params)  WHERE key = 'campaign') 
        END   AS utm_campaign,


        CASE
          WHEN (SELECT value.string_value FROM UNNEST(event_params)  WHERE key = 'term')        IN ('<Other>', '(data deleted)')  THEN 'Other'
          WHEN (SELECT value.string_value FROM UNNEST(event_params)  WHERE key = 'term')        IS NULL                           THEN 'Other'
          ELSE (SELECT value.string_value FROM UNNEST(event_params)  WHERE key = 'term') 
        END   AS utm_term,


        CASE
          WHEN (SELECT value.string_value FROM UNNEST(event_params)  WHERE key = 'content')     IN ('<Other>', '(data deleted)')  THEN 'Other'
          WHEN (SELECT value.string_value FROM UNNEST(event_params)  WHERE key = 'content')     IS NULL                           THEN 'Other'
          ELSE (SELECT value.string_value FROM UNNEST(event_params)  WHERE key = 'content') 
        END   AS utm_content,



        user_first_touch_timestamp,

        CASE
          WHEN traffic_source.source    IN ('<Other>', '(data deleted)')        THEN 'Other'   
          WHEN traffic_source.source    IS NULL                                 THEN 'Other' 
          ELSE traffic_source.source
        END AS first_user_source,

        
        CASE
          WHEN traffic_source.medium     IN ('<Other>', '(data deleted)')       THEN 'Other'   
          WHEN traffic_source.medium     IS NULL                                THEN 'Other' 
          ELSE traffic_source.medium 
        END AS first_user_medium,


        CASE
          WHEN traffic_source.name      IN ('<Other>', '(data deleted)')        THEN 'Other'   
          WHEN traffic_source.name      IS NULL                                 THEN 'Other' 
          ELSE traffic_source.name 
        END AS first_user_campaign,




        (SELECT value.int_value        FROM UNNEST (event_params)    WHERE key = 'entrances')  AS entrances,
        (SELECT value.string_value     FROM UNNEST (event_params)    WHERE key = 'page_title') AS page_title,


        event_name,

        (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'coupon')         AS coupon,
        (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'promotion_name') AS promotion_name,
        (SELECT value.int_value    FROM UNNEST (event_params) WHERE key = 'tax')            AS tax,
        (SELECT value.int_value    FROM UNNEST (event_params) WHERE key = 'value')          AS value,
        (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'shipping_tier')  AS shipping_tier,
 

        ecommerce.total_item_quantity,
        ecommerce.purchase_revenue,
        NULLIF(ecommerce.transaction_id, '(not set)') AS transaction_id,
        NULLIF(items.item_id,            '(not set)') AS item_id,
        NULLIF(items.item_name,          '(not set)') AS item_name,
        NULLIF(items.item_brand,         '(not set)') AS item_brand,
        NULLIF(items.item_variant,       '(not set)') AS item_variant,
        NULLIF(items.item_category,      '(not set)') AS item_category,
        items.price                                   AS item_price,                                                                                        
        items.quantity                                AS item_quantity,
        items.item_revenue                            AS item_revenue,

        NULLIF(items.item_list_id,       '(not set)') AS item_list_id,
        NULLIF(items.item_list_name,     '(not set)') AS item_list_name,
        NULLIF(items.item_list_index,    '(not set)') AS item_list_index,
        
       
        

FROM
  your_project.your_dataset.ga4_pubplic_dataset_ecommerce
  

LEFT JOIN
  UNNEST(items) AS items

)

SELECT
  *
FROM
 step1

)

