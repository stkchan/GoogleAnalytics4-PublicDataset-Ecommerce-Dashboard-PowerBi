CREATE OR REPLACE TABLE `your_project.your_dataset.your_table`
PARTITION BY event_date AS (

WITH
  prep_1 AS (

    SELECT
        event_date
      , user_pseudo_id
      , ga_session_id   AS session_number
      , utm_source
      , utm_medium
      , utm_campaign
      , utm_term
      , utm_content
     
      , ga_session_number

      , device_category
      , device_mobile_brand_name
      , device_operating_system
      , device_web_info_browser

      , geo_country
      , geo_region
      , geo_city

      , landing_page
      , page_location
      , page_title
      , page_referrer

      , event_name
    
      , CONCAT(user_pseudo_id, ga_session_id)                                                  AS session_id

      , MIN(event_timestamp)                                                                   AS start_time
      , MAX(event_timestamp)                                                                   AS end_time
      , MAX(session_engaged)                                                                   AS session_engaged_egagement_time
      , SUM(engagement_time_msec) / 1000                                                       AS engagement_time_seconds

      , COUNT(DISTINCT CONCAT(user_pseudo_id, ga_session_id))                                  AS sessions
      , COUNT(DISTINCT IF(session_engaged = '1', CONCAT(user_pseudo_id, ga_session_id), NULL)) AS engaged_sessions
      , COUNTIF(event_name = "page_view")                                                      AS Pageviews

    FROM
      `your_project.your_dataset.your_table`

    GROUP BY
        event_date
      , user_pseudo_id
      , session_number
      , utm_source
      , utm_medium
      , utm_campaign
      , utm_term
      , utm_content
      , session_id
      , device_category
      , device_mobile_brand_name
      , device_operating_system
      , device_web_info_browser
      , geo_country
      , geo_region
      , geo_city
      , landing_page
      , page_location
      , page_title
      , page_referrer
      , event_name
      , ga_session_number
)

SELECT

    event_date
  , utm_source
  , utm_medium
  , utm_campaign
  , utm_term
  , utm_content
  , session_id  AS total_sessions
  , ROUND(SAFE_DIVIDE(SUM(end_time - start_time) / 1000000, COUNT(DISTINCT session_id)), 2)                    AS average_session_duration
  , SAFE_DIVIDE(SUM(engagement_time_seconds), COUNT(DISTINCT CASE WHEN session_engaged_egagement_time = "1" THEN CONCAT(user_pseudo_id, session_number) END)) 
                                                                                                               AS average_engagement_time
  , ROUND(SAFE_DIVIDE(SUM(engaged_sessions), SUM(sessions)) * 100, 2)                                          AS engagement_rate
  , SUM(pageviews)                                                                                             AS total_pageviews
  , user_pseudo_id                                                                                             AS total_users


  , CASE WHEN  ga_session_number = 1     THEN user_pseudo_id ELSE NULL END                                     AS new_user
  , CASE WHEN  ga_session_number > 1     THEN user_pseudo_id ELSE NULL END                                     AS returning_user
  

  , device_category
  , device_mobile_brand_name
  , device_operating_system
  , device_web_info_browser

  , geo_country
  , geo_region
  , geo_city

  , landing_page
  , page_location
  , page_title
  

  , event_name

  FROM
    prep_1

  GROUP BY
      event_date
    , utm_source
    , utm_medium
    , utm_campaign
    , utm_term
    , utm_content
    , user_pseudo_id
    , session_id
    , ga_session_number
    , device_category
    , device_mobile_brand_name
    , device_operating_system
    , device_web_info_browser
    , geo_country
    , geo_region
    , geo_city
    , landing_page
    , page_location
    , page_title
    , event_name
    , new_user
    , returning_user

)



