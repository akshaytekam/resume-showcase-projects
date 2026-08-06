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

## Key Features:
### 1. Incremental Loading
Bronze and silver models use incremental materialization to process only new/changed data:
```text
{{ config(materialized='incremental') }}
{% if is_incremental() %}
    WHERE CREATED_AT > (SELECT COALESCE(MAX(CREATED_AT), '1900-01-01') FROM {{ this }})
{% endif %}
```

### 2. Custom Macros
Reusable business logic:
  - tag() macro: Categorizes prices into 'low', 'medium', 'high
```text
{{ tag('CAST(PRICE_PER_NIGHT AS INT)') }} AS PRICE_PER_NIGHT_TAG
```

### 3. Dynamic SQL Generation
The OBT (One Big Table) model uses Jinja loops for maintainable joins:
```text
{% set configs = [...] %}
SELECT {% for config in configs %}...{% endfor %}
```

### 4. Slowly Changing Dimensions
Track historical changes with timestamp-based snapshots:

- Valid from/to dates automatically maintained
- Historical data preserved for point-in-time analysis

### 5. Schema Organization
Automatic schema separation by layer:

- Bronze models → AIRBNB.BRONZE.*
- Silver models → AIRBNB.SILVER.*
- Gold models → AIRBNB.GOLD.*

## Data Quality:
### Testing Strategy
- Source data validation tests
- Unique key constraints
- Not null checks
- Referential integrity tests
- Custom business rule tests

### Data Lineage
dbt automatically tracks data lineage, showing:

- Upstream dependencies
- Downstream impacts
- Model relationships
- Source to consumption flow

### Security & Best Practices
- Credentials Management
  - Never commit profiles.yml with credentials
  - Use environment variables for sensitive data
  - Implement role-based access control (RBAC) in Snowflake

- Performance Optimization
  - Incremental models for large datasets
  - Ephemeral models for intermediate transformations
  - Appropriate clustering keys in Snowflake
