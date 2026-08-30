### What will the Job do?

We'll schedule the batch pipeline to run every day at 2:00 AM India time.

```text
02:00 AM
   ↓
Batch ingestion
   ↓
Silver transformation
   ↓
 ┌──────────────┐
 ↓              ↓
Revenue      Customers
```

NOTE:- We have to create and run DLT batch job and DLT streaming job differently

one job handles:- batch → Silver → Gold flow.
other job hadles:- The streaming pipeline should be managed separately through the streaming/DLT pipeline rather than starting the streaming notebook as an ordinary daily task.

```text
                     PRODUCTION
                         │
             ┌───────────┴───────────┐
             ↓                       ↓
       DAILY BATCH JOB          DLT PIPELINE
             │                       │
          COPY INTO             Auto Loader
             │                       │
             ↓                       ↓
          BRONZE                  BRONZE
             │                       │
             └───────────┬───────────┘
                         ↓
                       SILVER
                         ↓
                   ┌─────┴─────┐
                   ↓           ↓
                REVENUE     CUSTOMERS
                   │           │
                   └─────┬─────┘
                         ↓
                     DASHBOARD
```
