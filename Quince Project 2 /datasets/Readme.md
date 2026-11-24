# Understand the 11 source datasets:

Our datasets represent different operational systems within the Quince M2C business.

1. Orders – Storefront / E-commerce:
       - Records customer purchases and commercial terms. (Transactional source)
2. Order Events – Storefront Webhooks:
       - Captures order lifecycle events such as placement, allocation and factory packing. (Event source)
3. Factories – Supplier ERP:
       - Contains factory locations, audit status, quality ratings and lead times. (Master data)
4. Factory Capacity – Production Planning:
       - Indicates available production capacity by factory and SKU. (Operational data)
5. Raw Materials – Procurement ERP:
       - Contains raw material costs, supplier lead times and reorder points. (Master data)
6. Factory Production – Manufacturing ERP:
       - Tracks planned production, actual output and defective units. (Operational data)
7. Shipments – Carrier / 3PL Systems:
       - Tracks dispatch, delivery, carrier, freight cost and shipment status. (Event / logistics source)
8. Customs Tariffs – Customs Reference Feed:
       - Contains import duty rates based on origin, destination and SKU. (Reference data)
9. Freight Rates – Logistics Rate Cards:
        - Contains freight pricing and fuel surcharge percentages. (Reference data)
10. Returns – Returns Management System:
        - Tracks return reasons, refunds, product disposition and salvage recovery. (Post-sales data)
11. Payments – Payment Gateway:
        - Contains captured payments, gateway fees and refund amounts. (Financial transaction)

**"We integrated data from multiple business domains, including e-commerce, supplier ERP, manufacturing, logistics, customs, procurement and payment systems. Each source had a different data grain and refresh frequency. We established common business keys to integrate them into a unified analytical platform."**

```text
Our source grain:
Dataset	          Grain	                   Primary business key
-------                 -----                         -------------------
Orders	               One row/order	                     order_id
Order Events	        One row/event	                     event_id
Factories	       One row/factory	                     factory_id
Factory Capacity	One factory + SKU + date	              Composite key
Raw Materials	       One row/SKU	                            sku
Factory Production	One production run	                     production_id
Shipments	       One row/shipment	                     shipment_id
Customs Tariffs	(One origin + destination + SKU + effective date)	Composite key
Freight Rates	       (One origin + destination + rate date)	              Composite key
Returns	       One row/return	                     return_id
Payments	       One row/payment	                     payment_id
```

```text
We'll establish the following validation framework:

Dataset	              Validation rule	                     Action if invalid
------                      ---------------                           -----------------
Orders	                     order_id cannot be NULL	              Quarantine
Orders	                     Quantity must be greater than zero	       Quarantine
Orders	                     Selling price cannot be negative	       Quarantine
Order Events	              event_id must be unique	              Deduplicate
Order Events	              order_id must exist	                     Quarantine
Factories	              Factory ID must be unique	              Reject duplicate
Factories	              Ethical audit status must be valid	       Flag factory
Capacity	              Available capacity cannot be negative	Quarantine
Production	              Defects cannot exceed produced units	Quarantine
Shipments	              Shipment ID must be unique	              Deduplicate
Shipments	              Delivery cannot precede dispatch	       Quarantine
Tariffs	              Duty percentage must be within valid range	Quarantine
Freight	              Freight cost cannot be negative	       Quarantine
Returns	              Refund cannot be negative	              Quarantine
Payments	              Payment ID must be unique	              Deduplicate
```

## Important business rules:
These are the rules that distinguish our M2C project from an ordinary retail analytics project.

Rule 1 – Factory eligibility

A factory is eligible only when:
- It is active.
- Its ethical audit status is PASS.
- It has sufficient capacity for the requested SKU and quantity.
- It can satisfy the delivery SLA.
Rule 2 – Factory routing

Among eligible factories, we rank candidates using:
- Expected production lead time.
- Estimated freight.
- Customs duty.
- Quality score.
- Available production capacity.
The cheapest factory is not necessarily the best factory.

Rule 3 – True landed cost

For our modeled unit economics:
Landed Cost = material + factory + freight + custome

Rule 4 – Returns

A return may be initiated within the 365-day return window, subject to the business's eligibility policy.

We need to distinguish:
- Refund amount.
- Return logistics cost.
- Restockable value.
- Salvage recovery.
- Write-off.

Rule 5 – Capacity protection

An incoming order must not be allocated against capacity that has already been reserved by another order.
This requires reservation and concurrency controls in a production system, not just a SQL availability check.

```text
Interviewer: How did you design the data model for your M2C platform?

"I started by identifying the source systems and defining the grain of each dataset. The major sources were storefront orders, factory ERP, production capacity, carrier tracking, customs tariffs, freight rate cards, payments and returns.

I established common business keys such as order_id, factory_id, SKU and shipment_id. I also identified composite keys for datasets such as factory capacity and customs tariffs.

In the Silver layer, I standardized data types, deduplicated events, validated referential integrity and applied business rules.

In Gold, I created separate dimensional models for Supply Chain and Finance. The Supply Chain model focuses on factory performance, production, shipment and delivery. The Finance model focuses on revenue, discounts, landed costs, refunds, payment fees and contribution margin.

I also controlled join cardinality by aggregating one-to-many event and return datasets before joining them to order-level financial facts. This prevented revenue and cost duplication."
```
