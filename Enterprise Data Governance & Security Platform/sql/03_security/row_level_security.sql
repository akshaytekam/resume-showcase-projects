-- ============================================================
-- File        : row_level_security.sql
-- Project     : Enterprise Data Governance Platform
-- Purpose     : Row-Level Security implementation
-- ============================================================

USE CATALOG dev_catalog;

USE SCHEMA governance;

-- ============================================================
-- 1. USER/GROUP DATA ACCESS MAPPING
-- ============================================================

CREATE TABLE IF NOT EXISTS row_access_policy (

    policy_id BIGINT
        GENERATED ALWAYS AS IDENTITY,

    principal_name STRING NOT NULL,

    access_type STRING NOT NULL,

    access_value STRING NOT NULL,

    object_name STRING NOT NULL,

    is_active BOOLEAN DEFAULT TRUE,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

    created_by STRING

)
USING DELTA;

-- ============================================================
-- 2. REGIONAL ACCESS
-- ============================================================

INSERT INTO row_access_policy
(
    principal_name,
    access_type,
    access_value,
    object_name,
    is_active,
    created_by
)

VALUES

(
    'sales_west',
    'REGION',
    'WEST',
    'orders',
    TRUE,
    'governance_admin'
),

(
    'sales_east',
    'REGION',
    'EAST',
    'orders',
    TRUE,
    'governance_admin'
),

(
    'sales_north',
    'REGION',
    'NORTH',
    'orders',
    TRUE,
    'governance_admin'
),

(
    'sales_south',
    'REGION',
    'SOUTH',
    'orders',
    TRUE,
    'governance_admin'
);

INSERT INTO row_access_policy
(
    principal_name,
    access_type,
    access_value,
    object_name,
    is_active,
    created_by
)

VALUES

(
    'finance_india',
    'COUNTRY',
    'INDIA',
    'payments',
    TRUE,
    'governance_admin'
),

(
    'finance_us',
    'COUNTRY',
    'USA',
    'payments',
    TRUE,
    'governance_admin'
),

(
    'finance_europe',
    'COUNTRY',
    'UK',
    'payments',
    TRUE,
    'governance_admin'
);

INSERT INTO row_access_policy
(
    principal_name,
    access_type,
    access_value,
    object_name,
    is_active,
    created_by
)

VALUES

(
    'hr_recruitment',
    'DEPARTMENT',
    'RECRUITMENT',
    'employees',
    TRUE,
    'governance_admin'
),

(
    'hr_payroll',
    'DEPARTMENT',
    'PAYROLL',
    'employees',
    TRUE,
    'governance_admin'
),

(
    'hr_operations',
    'DEPARTMENT',
    'OPERATIONS',
    'employees',
    TRUE,
    'governance_admin'
);

-- ============================================================
-- 3. REGION ACCESS FUNCTION
-- ============================================================

CREATE OR REPLACE FUNCTION
dev_catalog.governance.has_region_access(
    requested_region STRING
)

RETURNS BOOLEAN

RETURN

    is_account_group_member('security_admins')

    OR

    is_account_group_member('data_governance_admins')

    OR

    EXISTS (

        SELECT 1

        FROM dev_catalog.governance.row_access_policy p

        WHERE

            p.principal_name = current_user()

            AND p.access_type = 'REGION'

            AND p.access_value = requested_region

            AND p.object_name = 'orders'

            AND p.is_active = TRUE

    );

CREATE OR REPLACE FUNCTION
dev_catalog.governance.has_region_access(
    requested_region STRING
)

RETURNS BOOLEAN

RETURN

    is_account_group_member('security_admins')

    OR

    is_account_group_member('data_governance_admins')

    OR

    (
        requested_region = 'WEST'
        AND is_account_group_member('sales_west')
    )

    OR

    (
        requested_region = 'EAST'
        AND is_account_group_member('sales_east')
    )

    OR

    (
        requested_region = 'NORTH'
        AND is_account_group_member('sales_north')
    )

    OR

    (
        requested_region = 'SOUTH'
        AND is_account_group_member('sales_south')
    );

-- ============================================================
-- 4. SECURE ORDERS VIEW
-- ============================================================

CREATE OR REPLACE VIEW
dev_catalog.gold.secure_orders
AS

SELECT

    order_id,

    order_date,

    customer_id,

    store_id,

    region,

    product_id,

    quantity,

    unit_price,

    total_amount,

    payment_method,

    order_status

FROM dev_catalog.silver.orders

WHERE
    dev_catalog.governance.has_region_access(
        region
    );

-- ============================================================
-- 5. COUNTRY ACCESS FUNCTION
-- ============================================================

CREATE OR REPLACE FUNCTION
dev_catalog.governance.has_country_access(
    requested_country STRING
)

RETURNS BOOLEAN

RETURN

    is_account_group_member('security_admins')

    OR

    is_account_group_member('data_governance_admins')

    OR

    (
        requested_country = 'INDIA'
        AND is_account_group_member('finance_india')
    )

    OR

    (
        requested_country = 'USA'
        AND is_account_group_member('finance_us')
    )

    OR

    (
        requested_country = 'UK'
        AND is_account_group_member('finance_europe')
    );

-- ============================================================
-- 6. SECURE PAYMENTS VIEW
-- ============================================================

CREATE OR REPLACE VIEW
dev_catalog.gold.secure_payments
AS

SELECT

    payment_id,

    order_id,

    customer_id,

    country,

    payment_method,

    payment_status,

    transaction_amount,

    currency,

    card_last4,

    payment_provider,

    transaction_timestamp

FROM dev_catalog.silver.payments

WHERE
    dev_catalog.governance.has_country_access(
        country
    );

-- ============================================================
-- 7. DEPARTMENT ACCESS FUNCTION
-- ============================================================

CREATE OR REPLACE FUNCTION
dev_catalog.governance.has_department_access(
    requested_department STRING
)

RETURNS BOOLEAN

RETURN

    is_account_group_member('security_admins')

    OR

    is_account_group_member('data_governance_admins')

    OR

    (
        requested_department = 'RECRUITMENT'
        AND is_account_group_member('hr_recruitment')
    )

    OR

    (
        requested_department = 'PAYROLL'
        AND is_account_group_member('hr_payroll')
    )

    OR

    (
        requested_department = 'OPERATIONS'
        AND is_account_group_member('hr_operations')
    );

-- ============================================================
-- 8. SECURE EMPLOYEES VIEW
-- ============================================================

CREATE OR REPLACE VIEW
dev_catalog.gold.secure_employees
AS

SELECT

    employee_id,

    email,

    phone_number,

    department,

    job_title,

    annual_salary,

    manager_id,

    joining_date,

    employment_status

FROM dev_catalog.silver.employees

WHERE
    dev_catalog.governance.has_department_access(
        department
    );

-- ============================================================
-- 9. SECURE VIEW ACCESS
-- ============================================================

GRANT USE SCHEMA
ON SCHEMA dev_catalog.gold
TO `data_analysts`;

GRANT SELECT
ON VIEW dev_catalog.gold.secure_orders
TO `data_analysts`;

GRANT SELECT
ON VIEW dev_catalog.gold.secure_payments
TO `finance_analysts`;

GRANT SELECT
ON VIEW dev_catalog.gold.secure_employees
TO `hr_analysts`;

-- ============================================================
-- 10. RLS POLICY REGISTRY
-- ============================================================

CREATE TABLE IF NOT EXISTS
row_security_policy_registry (

    policy_id BIGINT
        GENERATED ALWAYS AS IDENTITY,

    policy_name STRING NOT NULL,

    target_object STRING NOT NULL,

    filter_column STRING NOT NULL,

    filter_type STRING NOT NULL,

    authorized_groups STRING,

    description STRING,

    is_active BOOLEAN DEFAULT TRUE,

    created_at TIMESTAMP
        DEFAULT CURRENT_TIMESTAMP()

)
USING DELTA;

INSERT INTO row_security_policy_registry
(
    policy_name,
    target_object,
    filter_column,
    filter_type,
    authorized_groups,
    description
)

VALUES

(
    'ORDER_REGION_RLS',
    'dev_catalog.gold.secure_orders',
    'region',
    'REGION',
    'sales_west,sales_east,sales_north,sales_south',
    'Restricts order visibility by sales region'
),

(
    'PAYMENT_COUNTRY_RLS',
    'dev_catalog.gold.secure_payments',
    'country',
    'COUNTRY',
    'finance_india,finance_us,finance_europe',
    'Restricts payment visibility by country'
),

(
    'EMPLOYEE_DEPARTMENT_RLS',
    'dev_catalog.gold.secure_employees',
    'department',
    'DEPARTMENT',
    'hr_recruitment,hr_payroll,hr_operations',
    'Restricts employee visibility by department'
);

