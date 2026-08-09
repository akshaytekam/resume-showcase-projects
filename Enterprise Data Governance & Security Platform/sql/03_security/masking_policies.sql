-- ============================================================
-- File        : masking_policies.sql
-- Project     : Enterprise Data Governance Platform
-- Purpose     : Dynamic masking for sensitive data
-- ============================================================


-- ============================================================
-- 1. Catalog and Schema
-- ============================================================

USE CATALOG dev_catalog;

USE SCHEMA governance;

-- ============================================================
-- 2. EMAIL MASKING FUNCTION
-- ============================================================

CREATE OR REPLACE FUNCTION
dev_catalog.governance.mask_email(email STRING)

RETURNS STRING

RETURN
    CASE

        WHEN email IS NULL THEN NULL

        WHEN is_account_group_member(
            'security_admins'
        )
        THEN email

        WHEN is_account_group_member(
            'data_governance_admins'
        )
        THEN email

        ELSE
            regexp_replace(
                email,
                '^(.)([^@]*)(@.*)$',
                '$1********$3'
            )

  -- ============================================================
-- 3. PHONE MASKING FUNCTION
-- ============================================================

CREATE OR REPLACE FUNCTION
dev_catalog.governance.mask_phone(phone STRING)

RETURNS STRING

RETURN
    CASE

        WHEN phone IS NULL THEN NULL

        WHEN is_account_group_member(
            'security_admins'
        )
        THEN phone

        WHEN is_account_group_member(
            'data_governance_admins'
        )
        THEN phone

        ELSE
            CASE

                WHEN length(phone) >= 4

                THEN concat(
                    substring(phone, 1, 2),
                    '******',
                    substring(
                        phone,
                        length(phone) - 1,
                        2
                    )
                )

                ELSE '******'

            END

    END;

-- ============================================================
-- 4. DATE OF BIRTH MASKING
-- ============================================================

CREATE OR REPLACE FUNCTION
dev_catalog.governance.mask_date_of_birth(
    dob DATE
)

RETURNS DATE

RETURN
    CASE

        WHEN dob IS NULL THEN NULL

        WHEN is_account_group_member(
            'security_admins'
        )
        THEN dob

        WHEN is_account_group_member(
            'data_governance_admins'
        )
        THEN dob

        ELSE
            make_date(
                year(dob),
                1,
                1
            )

    END;

-- ============================================================
-- 5. SALARY MASKING
-- ============================================================

CREATE OR REPLACE FUNCTION
dev_catalog.governance.mask_salary(
    salary DECIMAL(18,2)
)

RETURNS DECIMAL(18,2)

RETURN
    CASE

        WHEN salary IS NULL THEN NULL

        WHEN is_account_group_member(
            'security_admins'
        )
        THEN salary

        WHEN is_account_group_member(
            'hr_analysts'
        )
        THEN round(
            salary,
            -4
        )

        ELSE NULL

    END;

-- ============================================================
-- 6. CARD LAST FOUR MASKING
-- ============================================================

CREATE OR REPLACE FUNCTION
dev_catalog.governance.mask_card_last4(
    card_last4 STRING
)

RETURNS STRING

RETURN
    CASE

        WHEN card_last4 IS NULL THEN NULL

        WHEN is_account_group_member(
            'security_admins'
        )
        THEN card_last4

        WHEN is_account_group_member(
            'finance_analysts'
        )
        THEN concat(
            '****'
        )

        ELSE
            '****'

    END;

-- ============================================================
-- 7. CUSTOMER ID MASKING
-- ============================================================

CREATE OR REPLACE FUNCTION
dev_catalog.governance.mask_customer_id(
    customer_id STRING
)

RETURNS STRING

RETURN
    CASE

        WHEN customer_id IS NULL THEN NULL

        WHEN is_account_group_member(
            'security_admins'
        )
        THEN customer_id

        WHEN is_account_group_member(
            'data_engineers'
        )
        THEN customer_id

        ELSE
            concat(
                'CUST',
                '****'
            )

    END;

-- ============================================================
-- 8. FINANCIAL AMOUNT MASKING
-- ============================================================

CREATE OR REPLACE FUNCTION
dev_catalog.governance.mask_financial_amount(
    amount DECIMAL(18,2)
)

RETURNS DECIMAL(18,2)

RETURN
    CASE

        WHEN amount IS NULL THEN NULL

        WHEN is_account_group_member(
            'security_admins'
        )
        THEN amount

        WHEN is_account_group_member(
            'finance_analysts'
        )
        THEN amount

        ELSE
            NULL

    END;

-- ============================================================
-- 9. MASKING POLICY REGISTRY
-- ============================================================

CREATE TABLE IF NOT EXISTS masking_policy_registry (

    policy_id BIGINT
        GENERATED ALWAYS AS IDENTITY,

    policy_name STRING NOT NULL,

    target_classification STRING NOT NULL,

    masking_function STRING NOT NULL,

    authorized_groups STRING,

    description STRING,

    created_at TIMESTAMP
        DEFAULT CURRENT_TIMESTAMP()

)
USING DELTA;

INSERT INTO masking_policy_registry
(
    policy_name,
    target_classification,
    masking_function,
    authorized_groups,
    description
)

VALUES

(
    'MASK_EMAIL',
    'PII',
    'dev_catalog.governance.mask_email',
    'security_admins,data_governance_admins',
    'Masks customer and employee email addresses'
),

(
    'MASK_PHONE',
    'PII',
    'dev_catalog.governance.mask_phone',
    'security_admins,data_governance_admins',
    'Masks telephone numbers'
),

(
    'MASK_DATE_OF_BIRTH',
    'SENSITIVE',
    'dev_catalog.governance.mask_date_of_birth',
    'security_admins,data_governance_admins',
    'Reduces precision of date of birth'
),

(
    'MASK_SALARY',
    'FINANCIAL',
    'dev_catalog.governance.mask_salary',
    'security_admins,hr_analysts',
    'Protects employee compensation'
),

(
    'MASK_CARD_LAST4',
    'SENSITIVE',
    'dev_catalog.governance.mask_card_last4',
    'security_admins',
    'Masks payment card information'
),

(
    'MASK_CUSTOMER_ID',
    'PII',
    'dev_catalog.governance.mask_customer_id',
    'security_admins,data_engineers',
    'Protects customer identifiers'
),

(
    'MASK_FINANCIAL_AMOUNT',
    'FINANCIAL',
    'dev_catalog.governance.mask_financial_amount',
    'security_admins,finance_analysts',
    'Protects financial transaction amounts'
);

-- ============================================================
-- 10. SECURE CUSTOMER VIEW
-- ============================================================

CREATE OR REPLACE VIEW
dev_catalog.gold.secure_customers
AS

SELECT

    customer_id,

    dev_catalog.governance.mask_email(
        email
    ) AS email,

    dev_catalog.governance.mask_phone(
        phone_number
    ) AS phone_number,

    dev_catalog.governance.mask_date_of_birth(
        date_of_birth
    ) AS date_of_birth,

    city,

    state

FROM dev_catalog.silver.customers;

-- ============================================================
-- 11. SECURE EMPLOYEE VIEW
-- ============================================================

CREATE OR REPLACE VIEW
dev_catalog.gold.secure_employees
AS

SELECT

    employee_id,

    dev_catalog.governance.mask_customer_id(
        employee_id
    ) AS employee_reference,

    dev_catalog.governance.mask_email(
        email
    ) AS email,

    department,

    job_title,

    dev_catalog.governance.mask_salary(
        annual_salary
    ) AS annual_salary,

    manager_id,

    joining_date,

    employment_status

FROM dev_catalog.silver.employees;

-- ============================================================
-- 12. SECURE PAYMENTS VIEW
-- ============================================================

CREATE OR REPLACE VIEW
dev_catalog.gold.secure_payments
AS

SELECT

    payment_id,

    order_id,

    dev_catalog.governance.mask_customer_id(
        customer_id
    ) AS customer_id,

    payment_method,

    payment_status,

    dev_catalog.governance.mask_financial_amount(
        transaction_amount
    ) AS transaction_amount,

    currency,

    dev_catalog.governance.mask_card_last4(
        card_last4
    ) AS card_last4,

    payment_provider,

    transaction_timestamp

FROM dev_catalog.silver.payments;



    END;
