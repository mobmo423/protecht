# protecht

Ingest data directly from Kaggle API using python files (extract.py) to Postgres
write sql models to transform. run the transformation witn (transform.py)
write python script to automate report to s3
use airflow (with docker) to orchestrate the daily jobs


q1_currency_momentum.sql:
This query calculates the top 5 currencies with the longest average streaks of consecutive exchange rate increases (avg_cons_pos_days) and the top 5 with the highest average percent change during those streaks (avg_cons_perc_change), then combines the results into a single table. It uses window functions and ranking logic to identify and aggregate meaningful momentum metrics from historical exchange rate data.

q2_behavior_clustering_metrics;
volatility_index: Normalized standard deviation — captures fluctuation intensity.

momentum_consistency: % of days that are part of upward streaks of 3+ days.

Cluster logic is simple threshold-based.

cd airflow
docker build -t protecht-airflow .
docker run -p 8080:8080 -v "%cd%:/opt/airflow" protecht-airflow airflow standalone

