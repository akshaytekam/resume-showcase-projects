# Quince end-to-end data engineering project

### We will build this as a realistic, portfolio-grade retail data platform based on Quince's publicly described business model, with three integrated pipelines:
1. Supply Chain Data Pipeline — factory production, inventory, carrier tracking, delivery, returns, and demand forecasting.
2. Finance Data Pipeline — orders, revenue, product costs, expenses, payment settlements, and profitability.
3. Treasury Data Pipeline — bank transactions, cash position, cash-flow forecasting, payment reconciliation, and financial risk.

**Project Stack**: Azure Data Factory (ADF), ADLS Gen2, Azure Databricks, PySpark, SQL, Python, Power BI, and MLflow, with Lakeflow Declarative Pipelines, Delta Lake, Change Data Capture (CDC), watermarking, centralized audit logging, schema validation, and liquid clustering.

## Understand Quince's business:
- **Business model**: Manufacturer-to-consumer (M2C) : Its a factory-direct model designed to remove traditional retail intermediaries.
- **Product categories**: Its public assortment includes apparel, accessories, jewelry, home goods, and other lifestyle categories. Our product model therefore needs category, SKU, variant, material, factory, and cost information.
- **Global manufacturing partners**: Quince working with specialist manufacturing partners across different regions and selecting factories for quality and responsible production.
- **Direct fulfillment and tracking**: Quince describes shipping directly from manufacturers to customers. Its logistics subsidiary also describes capabilities spanning planning, first-mile and last-mile logistics, tracking, returns, and fulfillment. These support our supply-chain use cases.

