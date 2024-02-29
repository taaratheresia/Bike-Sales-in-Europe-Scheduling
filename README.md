# Bike Sales in Europe Scheduling

This project is created as part of an assignment or personal project to analyze bicycle sales data in Europe from 2013 to 2016. The aim is to understand sales performance, customer behavior, market trends, and product stock optimization. The dataset on bicycle sales in Europe provides crucial insights into sales performance, market trends, and customer needs. This analysis aids in business decision-making to enhance profitability and efficiency.

Problems Solved
- Analysis of bicycle sales performance year over year.
- Profiling of customers likely to purchase bicycles.
- Identification of top-selling products and stock optimization.
- Analysis of profit and sales costs.
- Sales analysis across different countries or regions.

Project Outputs
- Sales performance analysis.
- Customer profiles.
- Identification of top-selling products.
- Profit and cost analysis.
- Sales analysis by region.

Methods Used
- ETL (Extract, Transform, Load) from PostgreSQL to Elasticsearch.
- Data cleansing using Great Expectations.
- Directed Acyclic Graph (DAG) creation using Apache Airflow.

Stack Used
- PostgreSQL
- Elasticsearch
- Great Expectations
- Apache Airflow

Files and Folder Contents
- P2M3_taara_DAG.py: Contains the DAG definition to manage the ETL workflow.
- P2M3_taara_GX.ipynb: Jupyter Notebook for data cleansing with Great Expectations.
- P2M3_taara_data_clean.csv: Cleansed data.
- P2M3_taara_data_raw.csv: Raw data from PostgreSQL.
- P2M3_taara_ddl.sql: Script for defining tables in PostgreSQL.
- Dashboard folder: May contain files related to dashboard creation for visualizing analysis results.

Project Advantages and Disadvantages
Advantages:
- Automation of ETL and data analysis processes.
- Use of Great Expectations for data validation.
- Integration with Apache Airflow for task scheduling.
Disadvantages:
- No mention of integration with a dashboard or data visualization.
- May require additional analysis to gain deeper insights.

References

Dataset: https://www.kaggle.com/datasets/sadiqshah/bike-sales-in-europe


