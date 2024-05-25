# GoogleAnalytics4-PublicDataset-Ecommerce-Dashboard-PowerBi
![image](https://github.com/stkchan/GoogleAnalytics4-PublicDataset-Ecommerce-Dashboard-PowerBi/blob/main/Traffic_Performance_report.png)

## Table of Contents
* [Project Objective](#Project-Objective)
* [Dataset](#Dataset)
* [ETL Process](#ETL-Process)
* [DAX Formulas](#DAX-Formulas)
* [Question](#Question)
* [Conclusion](#Conclusion)

## Project Objective
In this project, we aim to find insights from the Google Merchandise Store GA4 eCommerce public dataset. The primary objective is to use ETL (Extract, Transform, Load) by extracting data in BigQuery and visualizing it in Power BI to generate insights based on defined goals.


## Dataset
The dataset for this project is from the [Google Merchandise Store](https://shop.googlemerchandisestore.com/), an online store that sells Google-branded merchandise. You can find the dataset [here](https://developers.google.com/analytics/bigquery/web-ecommerce-demo-dataset), The dataset, available through the BigQuery Public Datasets program, contains a sample of obfuscated BigQuery event export data for three months from 2020-11-01 to 2021-01-31. It includes many columns such as event_date, user_pseudo_id (client ID in Google Analytics 4), device_category, device_brand, geo_region, geo_city, and eCommerce dimensions such as transaction_id, item_id (SKU), item_name, item_category, item_revenue, purchase revenue, and more.

You can find GA4 BigQuery schema :[here](https://support.google.com/analytics/answer/7029846?hl=en)

## ETL Process
* **Overall** - In the first step, I extracted the columns from the dataset that I wanted to use and unnested some columns that are stored as arrays, which is the normal format in Google Analytics 4 BigQuery. You can see my scrip [SQL](https://github.com/stkchan/GoogleAnalytics4-PublicDataset-Ecommerce-Dashboard-PowerBi/blob/main/unnested_table.sql) After ensuring there are no nested columns, I will create more tables to be used in Power BI for analysis in the next step.
* **Traffic Performance report** - In this step, I created a table for analyzing traffic performance. You can see my script [SQL](https://github.com/stkchan/GoogleAnalytics4-PublicDataset-Ecommerce-Dashboard-PowerBi/blob/main/traffic_performance.sql) I calculated the total number of users, sessions, pageviews, engagement rate, and more by grouping by UTM source, medium, and campaign to monitor traffic channels, and identify which device category and web browser have the most users. This provides an overview of performance in terms of users, sessions, and pageviews over three months.
* **Ecommerce Funnel report** - I have calculated the number of events at each step before customers complete their purchase and transaction in the [Google Merchandise Store](https://shop.googlemerchandisestore.com/) This was done by grouping each traffic channel for each step (session_start, view_item, select_item, add_to_cart, begin_checkout, add_payment_info, purchase) SQL Script is  [here](https://github.com/stkchan/GoogleAnalytics4-PublicDataset-Ecommerce-Dashboard-PowerBi/blob/main/ecommerce_funnel.sql)
   - Note: session_start is an event in GA4 that occurs when users enter the website and initiate a session. A session will time out after 30 minutes of inactivity."
* **Customer Journey by Pages** - In this step, I calculated the total number of users for each page on the website to determine which page has the most users. [SQL Script](https://github.com/stkchan/GoogleAnalytics4-PublicDataset-Ecommerce-Dashboard-PowerBi/blob/main/customerJourney_byPages.sql)
  
