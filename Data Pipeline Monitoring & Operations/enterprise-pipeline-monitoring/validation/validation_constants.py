"""
validation_constants.py

Stores validation rules used across all datasets.
"""

EXPECTED_SALES_COLUMNS = [
    "sale_id",
    "store_id",
    "customer_id",
    "product_id",
    "sale_date",
    "quantity",
    "price"
]

PRIMARY_KEYS = {
    "sales": ["sale_id"],
    "customers": ["customer_id"],
    "products": ["product_id"],
    "inventory": ["product_id", "store_id"]
}

FOREIGN_KEYS = {
    "sales": {
        "customer_id": "customers.customer_id",
        "product_id": "products.product_id"
    }
}

NON_NULL_COLUMNS = {
    "sales": [
        "sale_id",
        "store_id",
        "customer_id",
        "product_id",
        "sale_date"
    ]
}

BUSINESS_RULES = {

    "quantity_positive": True,

    "price_positive": True,

    "future_date_not_allowed": True

}
