# Airbnb Data Pipeline Project Using Snowflake and DBT

This project implements a complete end-to-end data engineering pipeline for Airbnb data using modern cloud technologies. 
The solution demonstrates best practices in data warehousing, transformation, and analytics using Snowflake, DBT and AWS. 
We'll gonna use open source DTB core (DBT CLI) for this project.

The pipeline processes Airbnb listings, bookings, and hosts data through 
a medallion architecture (Bronze → Silver → Gold), implementing incremental loading, slowly changing dimensions (SCD Type 2), 
and creating analytics-ready datasets.

---


### Tech-Stack and Data Flow:
```text
Source Data (CSV) → AWS S3 → Snowflake (Staging) → Bronze Layer → Silver Layer → Gold Layer
                                                           ↓              ↓           ↓
                                                      Raw Tables    Cleaned Data   Analytics
```
### Key dbt Features:
- Incremental models
- Snapshots (SCD Type 2)
- Custom macros
- Jinja templating
- Testing and documentation

## Data Model
### Medallion Architecture
#### Bronze Layer (Raw Data)
Raw data ingested from staging with minimal transformations:
- bronze_bookings - Raw booking transactions
- bronze_hosts - Raw host information
- bronze_listings - Raw property listings

#### Silver Layer (Cleaned Data)
Cleaned and standardized data:
- silver_bookings - Validated booking records
- silver_hosts - Enhanced host profiles with quality metrics
- silver_listings - Standardized listing information with price categorization

#### Gold Layer (Analytics-Ready) (We'll create OBT & Star Schema both)
Business-ready datasets optimized for analytics:
- obt (One Big Table) - Denormalized fact table joining bookings, listings, and hosts
- fact - Fact table for dimensional modeling
- Ephemeral models for intermediate transformations

### Snapshots (SCD Type 2)
Slowly Changing Dimensions to track historical changes:

- dim_bookings - Historical booking changes
- dim_hosts - Historical host profile changes
- dim_listings - Historical listing changes

## Project Structure:
```text
AWS_DBT_Snowflake/
├── README.md                           # This file
├── pyproject.toml                      # Python dependencies
├── main.py                             # Main execution script
│
├── SourceData/                         # Raw CSV data files
│   ├── bookings.csv
│   ├── hosts.csv
│   └── listings.csv
│
├── DDL/                                # Database schema definitions
│   ├── ddl.sql                         # Table creation scripts
│   └── resources.sql
│
└── aws_dbt_snowflake_project/         # Main dbt project
    ├── dbt_project.yml                 # dbt project configuration
    ├── ExampleProfiles.yml             # Snowflake connection profile
    │
    ├── models/                         # dbt models
    │   ├── sources/
    │   │   └── sources.yml             # Source definitions
    │   ├── bronze/                     # Raw data layer
    │   │   ├── bronze_bookings.sql
    │   │   ├── bronze_hosts.sql
    │   │   └── bronze_listings.sql
    │   ├── silver/                     # Cleaned data layer
    │   │   ├── silver_bookings.sql
    │   │   ├── silver_hosts.sql
    │   │   └── silver_listings.sql
    │   └── gold/                       # Analytics layer
    │       ├── fact.sql
    │       ├── obt.sql
    │       └── ephemeral/              # Temporary models
    │           ├── bookings.sql
    │           ├── hosts.sql
    │           └── listings.sql
    │
    ├── macros/                         # Reusable SQL functions
    │   ├── generate_schema_name.sql    # Custom schema naming
    │   ├── multiply.sql                # Math operations
    │   ├── tag.sql                     # Categorization logic
    │   └── trimmer.sql                 # String utilities
    │
    ├── analyses/                       # Ad-hoc analysis queries
    │   ├── explore.sql
    │   ├── if_else.sql
    │   └── loop.sql
    │
    ├── snapshots/                      # SCD Type 2 configurations
    │   ├── dim_bookings.yml
    │   ├── dim_hosts.yml
    │   └── dim_listings.yml
    │
    ├── tests/                          # Data quality tests
    │   └── source_tests.sql
    │
    └── seeds/                          # Static reference data
```


## STEP - 1
- Create AWS S3 Bucket 
- Now within that bucket create folders/containers for separate data.

## STEP - 2
- Create Snowflake's Database, Schemas and Tables
- Then do the integration between AWS S3 and Snowflake.
- We can create the separate external stages for each folder in S3 bucket.
- Create the IAM role for snowflake connectivity. ANd also create the Access Key for the user to access the folder.
- And now using COPY INTO command insert all data to the tables through staging.

## STEP - 3
- Now install DBT on VS Code editor using virtual env. for transformation.
      - DBT just provides us the modularised coding templete for transformations.
- Now connect the DBT and Snowflake.
