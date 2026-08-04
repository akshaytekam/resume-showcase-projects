# Airbnb Data Pipeline Project Using Snowflake and DBT

This project implements a complete end-to-end data engineering pipeline for Airbnb data using modern cloud technologies. 
The solution demonstrates best practices in data warehousing, transformation, and analytics using Snowflake, DBT and AWS. 
We'll gonna use open source DTB core (DBT CLI) for this project.

The pipeline processes Airbnb listings, bookings, and hosts data through 
a medallion architecture (Bronze → Silver → Gold), implementing incremental loading, slowly changing dimensions (SCD Type 2), 
and creating analytics-ready datasets.
---


### Tech-Stack:
```text
AWS S3 (Data Lake) --> Snowflake --> DBT --> Github
```

## STEP - 1
- Create AWS S3 Bucket 
- Now within that bucket create folders/containers for separate data.
