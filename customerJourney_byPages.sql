CREATE OR REPLACE TABLE your_project.your_dataset.your_table AS (

SELECT 

  *

FROM
  (
    SELECT

      COUNT (DISTINCT users) AS users
      , utm_source
      , utm_medium
      , utm_campaign
      , yearMonth
      , page1,page2,page3, page4, page5, page6, page7, page8, page9, page10
    

    FROM (

WITH 
  step_1 AS(
    SELECT

        FORMAT_DATE('%Y-%m', DATE(event_date)) AS yearMonth
      , CONCAT(user_pseudo_id, ga_session_id)  AS users
      , page_title
      , event_timestamp
      , utm_source
      , utm_medium
      , utm_campaign

    FROM
      `your_project.your_dataset.your_table`

    WHERE 
          1=1
      AND event_name = 'page_view'
  )

, create_rank AS (

  SELECT

      *
    , RANK()                OVER (PARTITION BY users ORDER BY event_timestamp) AS rank
    , LEAD(event_timestamp) OVER (PARTITION BY users ORDER BY event_timestamp) AS time_next
    , LEAD(page_title)   OVER (PARTITION BY users ORDER BY event_timestamp) AS page_next   

  FROM
    step_1
  
  ORDER BY
    users
  , rank

)

, check_event AS (

  SELECT

    *
  , CASE
      WHEN page_title = page_next THEN 0 
      ELSE 1
    END AS checking
  
  FROM
    create_rank
)

, all_event AS (

  SELECT

    *
  , RANK() OVER (PARTITION BY users ORDER BY rank) AS new_rank
  , (time_next - event_timestamp) AS time_used

  FROM
    check_event

  WHERE
        1=1
    AND checking = 1

  ORDER BY
    users
  , event_timestamp
)

, semi_final AS (

  SELECT

      all_event.yearMonth
    , all_event.users
    , all_event.utm_source
    , all_event.utm_medium
    , all_event.utm_campaign
    , all_event.page_title AS page1
    , LEAD(all_event.page_title, 1) OVER (PARTITION BY all_event.yearMonth, all_event.users ORDER BY all_event.new_rank) AS page2
    , LEAD(all_event.page_title, 2) OVER (PARTITION BY all_event.yearMonth, all_event.users ORDER BY all_event.new_rank) AS page3
    , LEAD(all_event.page_title, 3) OVER (PARTITION BY all_event.yearMonth, all_event.users ORDER BY all_event.new_rank) AS page4
    , LEAD(all_event.page_title, 4) OVER (PARTITION BY all_event.yearMonth, all_event.users ORDER BY all_event.new_rank) AS page5
    , LEAD(all_event.page_title, 5) OVER (PARTITION BY all_event.yearMonth, all_event.users ORDER BY all_event.new_rank) AS page6
    , LEAD(all_event.page_title, 6) OVER (PARTITION BY all_event.yearMonth, all_event.users ORDER BY all_event.new_rank) AS page7
    , LEAD(all_event.page_title, 7) OVER (PARTITION BY all_event.yearMonth, all_event.users ORDER BY all_event.new_rank) AS page8
    , LEAD(all_event.page_title, 8) OVER (PARTITION BY all_event.yearMonth, all_event.users ORDER BY all_event.new_rank) AS page9
    , LEAD(all_event.page_title, 9) OVER (PARTITION BY all_event.yearMonth, all_event.users ORDER BY all_event.new_rank) AS page10
    , all_event.new_rank AS rank
  
  FROM
    all_event
  
  ORDER BY
    2

)

SELECT 
  *
FROM

 semi_final

WHERE
  rank = 1

)
GROUP BY ALL

ORDER BY 
  1 DESC

)
)

