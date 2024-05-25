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

## ETL Process
* **Traffic Performance report** - 
