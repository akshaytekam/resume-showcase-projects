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
