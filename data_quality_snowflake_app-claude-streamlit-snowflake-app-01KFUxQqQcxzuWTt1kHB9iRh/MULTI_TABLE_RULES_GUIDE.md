# Multi-Table Dependency Rules Guide

**Version:** 1.1
**Last Updated:** 2025-11-23

---

## 🎯 Problem: Column Values Depend on Multiple Tables

This guide shows how to validate complex business rules where **one column's value depends on data from MULTIPLE other tables**.

### Common Scenarios

1. **Aggregation Dependencies**
   - Order.total_amount should equal SUM(OrderLines.price)
   - Customer.total_spent should equal SUM(Orders.amount)
   - Product.stock_quantity should equal SUM(Inventory.quantity)

2. **Conditional Dependencies**
   - Customer.status = 'VIP' ONLY IF total purchases > $10,000
   - Order.can_ship = TRUE ONLY IF product exists AND warehouse has stock
   - Employee.bonus > 0 ONLY IF department budget allows

---

## 🔧 Solution 1: MULTI_TABLE_AGGREGATE

Validate that a column matches an aggregation from related tables.

### Scenario A: Order Total Must Equal Sum of Line Items

**Database Schema:**
```sql
-- Main table: ORDERS
CREATE TABLE ORDERS (
    order_id NUMBER PRIMARY KEY,
    customer_id NUMBER,
    total_amount NUMBER,      -- Should equal sum of lines!
    order_date DATE
);

-- Related table: ORDER_LINES
CREATE TABLE ORDER_LINES (
    order_id NUMBER,
    line_number NUMBER,
    product_id NUMBER,
    quantity NUMBER,
    unit_price NUMBER,
    PRIMARY KEY (order_id, line_number)
);

-- Test data with violations
INSERT INTO ORDERS VALUES
    (1, 100, 150.00, '2024-01-01'),   -- Correct
    (2, 101, 999.99, '2024-01-02');   -- WRONG! Should be 75.00

INSERT INTO ORDER_LINES VALUES
    (1, 1, 'A', 2, 50.00),  -- 2 * 50 = 100
    (1, 2, 'B', 1, 50.00),  -- 1 * 50 = 50
                             -- Total: 150 ✅ Matches!

    (2, 1, 'C', 3, 25.00);  -- 3 * 25 = 75
                             -- Total: 75 ❌ But ORDERS says 999.99!
```

**YAML Definition:**
```yaml
semantic_view:
  name: "order_integrity_check"
  source:
    database: "MYDB"
    schema: "SALES"
    table: "ORDERS"

  table_rules:
    - id: "order_total_matches_lines"
      type: "MULTI_TABLE_AGGREGATE"
      severity: "CRITICAL"
      description: "Order total must equal sum of line items"
      params:
        target_column: "total_amount"  # Column to validate

        aggregate_expr: "SUM(ol.quantity * ol.unit_price)"  # Calculation

        related_tables:
          - table: "ORDER_LINES"
            alias: "ol"
            join_type: "LEFT JOIN"

        join_conditions:
          - "main.order_id = ol.order_id"

        tolerance: 0.01  # Allow 1 cent difference (rounding)
```

**SQL Generated:**
```sql
WITH aggregated AS (
    SELECT
        main.*,
        SUM(ol.quantity * ol.unit_price) as calculated_value
    FROM MYDB.SALES.ORDERS main
    LEFT JOIN MYDB.SALES.ORDER_LINES ol ON main.order_id = ol.order_id
    GROUP BY main.*
)
SELECT *
FROM aggregated
WHERE ABS(COALESCE(total_amount, 0) - COALESCE(calculated_value, 0)) > 0.01
```

**Results:**
```
Violations found: 1

order_id | total_amount | calculated_value | difference
---------|--------------|------------------|------------
2        | 999.99       | 75.00           | 924.99     ← VIOLATION!
```

---

### Scenario B: Customer Total Spent Across Multiple Tables

**Database Schema:**
```sql
-- Main table: CUSTOMERS
CREATE TABLE CUSTOMERS (
    customer_id NUMBER PRIMARY KEY,
    customer_name VARCHAR,
    total_lifetime_value NUMBER  -- Should equal all orders!
);

-- Related table 1: ORDERS
CREATE TABLE ORDERS (
    order_id NUMBER PRIMARY KEY,
    customer_id NUMBER,
    order_amount NUMBER
);

-- Related table 2: SUBSCRIPTIONS
CREATE TABLE SUBSCRIPTIONS (
    subscription_id NUMBER PRIMARY KEY,
    customer_id NUMBER,
    monthly_fee NUMBER,
    months_active NUMBER
);
```

**YAML Definition:**
```yaml
table_rules:
  - id: "customer_ltv_check"
    type: "MULTI_TABLE_AGGREGATE"
    severity: "WARNING"
    description: "Customer LTV = orders + subscriptions"
    params:
      target_column: "total_lifetime_value"

      # Sum from TWO different tables!
      aggregate_expr: >
        COALESCE(SUM(o.order_amount), 0) +
        COALESCE(SUM(s.monthly_fee * s.months_active), 0)

      related_tables:
        - table: "ORDERS"
          alias: "o"
          join_type: "LEFT JOIN"
        - table: "SUBSCRIPTIONS"
          alias: "s"
          join_type: "LEFT JOIN"

      join_conditions:
        - "main.customer_id = o.customer_id"
        - "main.customer_id = s.customer_id"

      tolerance: 1.00  # Allow $1 difference
```

**Results:**
```
Customer 101:
  Stored LTV: $5,000
  Calculated: $2,500 (orders) + $2,400 (subscriptions) = $4,900
  Difference: $100
  Status: ✅ PASS (within $1 tolerance)

Customer 102:
  Stored LTV: $10,000
  Calculated: $3,000 (orders) + $1,200 (subscriptions) = $4,200
  Difference: $5,800
  Status: ❌ VIOLATION
```

---

### Scenario C: Inventory Count Across Warehouses

**Database Schema:**
```sql
-- Main table: PRODUCTS
CREATE TABLE PRODUCTS (
    product_id NUMBER PRIMARY KEY,
    product_name VARCHAR,
    total_stock NUMBER  -- Should equal sum across all warehouses!
);

-- Related table: WAREHOUSE_INVENTORY
CREATE TABLE WAREHOUSE_INVENTORY (
    warehouse_id NUMBER,
    product_id NUMBER,
    quantity NUMBER,
    PRIMARY KEY (warehouse_id, product_id)
);
```

**YAML Definition:**
```yaml
table_rules:
  - id: "product_stock_check"
    type: "MULTI_TABLE_AGGREGATE"
    severity: "CRITICAL"
    description: "Product stock = sum of all warehouses"
    params:
      target_column: "total_stock"
      aggregate_expr: "SUM(w.quantity)"

      related_tables:
        - table: "WAREHOUSE_INVENTORY"
          alias: "w"
          join_type: "LEFT JOIN"

      join_conditions:
        - "main.product_id = w.product_id"

      tolerance: 0  # Must match exactly (no tolerance for inventory)
```

---

## 🎯 Solution 2: MULTI_TABLE_CONDITION

Validate complex conditional business rules across multiple tables.

### Scenario A: VIP Status Based on Purchase History

**Business Rule:**
> "A customer can only have status='VIP' if they have spent more than $10,000 total"

**Database Schema:**
```sql
-- Main table: CUSTOMERS
CREATE TABLE CUSTOMERS (
    customer_id NUMBER PRIMARY KEY,
    customer_name VARCHAR,
    status VARCHAR  -- 'REGULAR', 'VIP', 'PLATINUM'
);

-- Related table: ORDERS
CREATE TABLE ORDERS (
    order_id NUMBER,
    customer_id NUMBER,
    amount NUMBER
);

-- Test data with violation
INSERT INTO CUSTOMERS VALUES
    (1, 'Alice', 'VIP'),      -- Spent $15,000 ✅ Valid
    (2, 'Bob', 'VIP'),        -- Spent $500 ❌ VIOLATION!
    (3, 'Charlie', 'REGULAR'); -- Spent $5,000 ✅ Valid

INSERT INTO ORDERS VALUES
    (1, 1, 15000),  -- Alice
    (2, 2, 500),    -- Bob
    (3, 3, 5000);   -- Charlie
```

**YAML Definition:**
```yaml
table_rules:
  - id: "vip_status_validation"
    type: "MULTI_TABLE_CONDITION"
    severity: "WARNING"
    description: "VIP status requires >$10K total purchases"
    params:
      # Condition: IF status = 'VIP', THEN total orders > 10000
      condition: >
        main.status != 'VIP' OR
        COALESCE(SUM(o.amount), 0) >= 10000

      related_tables:
        - table: "ORDERS"
          alias: "o"
          join_type: "LEFT JOIN"

      join_conditions:
        - "main.customer_id = o.customer_id"
```

**SQL Generated:**
```sql
SELECT main.*
FROM CUSTOMERS main
LEFT JOIN ORDERS o ON main.customer_id = o.customer_id
WHERE NOT (
    main.status != 'VIP' OR
    COALESCE(SUM(o.amount), 0) >= 10000
)
```

**Results:**
```
Violations found: 1

customer_id | customer_name | status | total_orders
------------|---------------|--------|-------------
2           | Bob           | VIP    | $500        ← VIOLATION!

Reason: Status is 'VIP' but total purchases ($500) < $10,000
```

---

### Scenario B: Shipping Validation

**Business Rule:**
> "An order can only be marked 'SHIPPED' if ALL line items have sufficient inventory"

**Database Schema:**
```sql
CREATE TABLE ORDERS (
    order_id NUMBER,
    status VARCHAR  -- 'PENDING', 'SHIPPED', 'DELIVERED'
);

CREATE TABLE ORDER_LINES (
    order_id NUMBER,
    product_id NUMBER,
    quantity_ordered NUMBER
);

CREATE TABLE INVENTORY (
    product_id NUMBER,
    quantity_available NUMBER
);
```

**YAML Definition:**
```yaml
table_rules:
  - id: "shipping_validation"
    type: "MULTI_TABLE_CONDITION"
    severity: "CRITICAL"
    description: "Can only ship if inventory available"
    params:
      condition: >
        main.status != 'SHIPPED' OR
        (
          COUNT(CASE WHEN ol.quantity_ordered > inv.quantity_available THEN 1 END) = 0
        )

      related_tables:
        - table: "ORDER_LINES"
          alias: "ol"
          join_type: "LEFT JOIN"
        - table: "INVENTORY"
          alias: "inv"
          join_type: "LEFT JOIN"

      join_conditions:
        - "main.order_id = ol.order_id"
        - "ol.product_id = inv.product_id"
```

---

### Scenario C: Employee Bonus Validation

**Business Rule:**
> "An employee can only receive a bonus if:
> - Performance rating >= 4.0 AND
> - Department budget has remaining funds AND
> - Employee has been with company > 1 year"

**YAML Definition:**
```yaml
table_rules:
  - id: "bonus_eligibility"
    type: "MULTI_TABLE_CONDITION"
    severity: "WARNING"
    description: "Bonus eligibility checks"
    params:
      condition: >
        main.bonus_amount = 0 OR (
          pr.rating >= 4.0 AND
          dept.budget_remaining >= main.bonus_amount AND
          DATEDIFF(day, main.hire_date, CURRENT_DATE()) > 365
        )

      related_tables:
        - table: "PERFORMANCE_REVIEWS"
          alias: "pr"
          join_type: "LEFT JOIN"
        - table: "DEPARTMENTS"
          alias: "dept"
          join_type: "LEFT JOIN"

      join_conditions:
        - "main.employee_id = pr.employee_id AND pr.review_year = YEAR(CURRENT_DATE())"
        - "main.department_id = dept.department_id"
```

---

## 📊 Complete Example: Order Management System

### Full Database Schema

```sql
-- Customers
CREATE TABLE CUSTOMERS (
    customer_id NUMBER PRIMARY KEY,
    customer_name VARCHAR,
    email VARCHAR,
    status VARCHAR,  -- 'REGULAR', 'VIP', 'PLATINUM'
    total_lifetime_value NUMBER
);

-- Orders
CREATE TABLE ORDERS (
    order_id NUMBER PRIMARY KEY,
    customer_id NUMBER,
    order_date DATE,
    total_amount NUMBER,
    status VARCHAR,  -- 'PENDING', 'SHIPPED', 'DELIVERED'
    can_ship BOOLEAN
);

-- Order Lines
CREATE TABLE ORDER_LINES (
    order_id NUMBER,
    line_number NUMBER,
    product_id NUMBER,
    quantity NUMBER,
    unit_price NUMBER,
    PRIMARY KEY (order_id, line_number)
);

-- Products
CREATE TABLE PRODUCTS (
    product_id NUMBER PRIMARY KEY,
    product_name VARCHAR,
    category VARCHAR,
    total_stock NUMBER
);

-- Inventory (multiple warehouses)
CREATE TABLE WAREHOUSE_INVENTORY (
    warehouse_id NUMBER,
    product_id NUMBER,
    quantity NUMBER,
    PRIMARY KEY (warehouse_id, product_id)
);
```

### Complete YAML with All Multi-Table Rules

```yaml
# ============================================================================
# ORDERS Table Validation
# ============================================================================
semantic_view:
  name: "orders_multi_table_check"
  source:
    database: "ECOMMERCE"
    schema: "SALES"
    table: "ORDERS"

  table_rules:
    # Rule 1: Order total = sum of lines
    - id: "order_total_validation"
      type: "MULTI_TABLE_AGGREGATE"
      severity: "CRITICAL"
      description: "Order total must match sum of line items"
      params:
        target_column: "total_amount"
        aggregate_expr: "SUM(ol.quantity * ol.unit_price)"
        related_tables:
          - table: "ORDER_LINES"
            alias: "ol"
            join_type: "LEFT JOIN"
        join_conditions:
          - "main.order_id = ol.order_id"
        tolerance: 0.01

    # Rule 2: Can only ship if inventory available
    - id: "shipping_inventory_check"
      type: "MULTI_TABLE_CONDITION"
      severity: "CRITICAL"
      description: "Can ship only if all products in stock"
      params:
        condition: >
          main.can_ship = FALSE OR (
            COUNT(CASE
              WHEN ol.quantity > COALESCE(inv.quantity, 0)
              THEN 1
            END) = 0
          )
        related_tables:
          - table: "ORDER_LINES"
            alias: "ol"
            join_type: "LEFT JOIN"
          - table: "WAREHOUSE_INVENTORY"
            alias: "inv"
            join_type: "LEFT JOIN"
        join_conditions:
          - "main.order_id = ol.order_id"
          - "ol.product_id = inv.product_id"

# ============================================================================
# CUSTOMERS Table Validation
# ============================================================================
---
semantic_view:
  name: "customers_multi_table_check"
  source:
    table: "CUSTOMERS"

  table_rules:
    # Rule 3: Customer LTV = sum of all orders
    - id: "customer_ltv_validation"
      type: "MULTI_TABLE_AGGREGATE"
      severity: "WARNING"
      description: "Customer LTV must match order history"
      params:
        target_column: "total_lifetime_value"
        aggregate_expr: "COALESCE(SUM(o.total_amount), 0)"
        related_tables:
          - table: "ORDERS"
            alias: "o"
            join_type: "LEFT JOIN"
        join_conditions:
          - "main.customer_id = o.customer_id"
        tolerance: 1.00

    # Rule 4: VIP status requires minimum spend
    - id: "vip_status_validation"
      type: "MULTI_TABLE_CONDITION"
      severity: "WARNING"
      description: "VIP status requires $10K+ total orders"
      params:
        condition: >
          main.status != 'VIP' OR
          COALESCE(SUM(o.total_amount), 0) >= 10000
        related_tables:
          - table: "ORDERS"
            alias: "o"
            join_type: "LEFT JOIN"
        join_conditions:
          - "main.customer_id = o.customer_id"

    # Rule 5: Platinum status requires more
    - id: "platinum_status_validation"
      type: "MULTI_TABLE_CONDITION"
      severity: "WARNING"
      description: "Platinum status requires $50K+ AND 20+ orders"
      params:
        condition: >
          main.status != 'PLATINUM' OR (
            COALESCE(SUM(o.total_amount), 0) >= 50000 AND
            COUNT(o.order_id) >= 20
          )
        related_tables:
          - table: "ORDERS"
            alias: "o"
            join_type: "LEFT JOIN"
        join_conditions:
          - "main.customer_id = o.customer_id"

# ============================================================================
# PRODUCTS Table Validation
# ============================================================================
---
semantic_view:
  name: "products_multi_table_check"
  source:
    table: "PRODUCTS"

  table_rules:
    # Rule 6: Product stock = sum across warehouses
    - id: "product_stock_validation"
      type: "MULTI_TABLE_AGGREGATE"
      severity: "CRITICAL"
      description: "Product stock must match warehouse totals"
      params:
        target_column: "total_stock"
        aggregate_expr: "COALESCE(SUM(inv.quantity), 0)"
        related_tables:
          - table: "WAREHOUSE_INVENTORY"
            alias: "inv"
            join_type: "LEFT JOIN"
        join_conditions:
          - "main.product_id = inv.product_id"
        tolerance: 0  # Must match exactly
```

---

## 🎯 Advanced Patterns

### Pattern 1: Three-Table Join

**Validate order discounts based on customer tier AND product category:**

```yaml
- type: "MULTI_TABLE_CONDITION"
  params:
    condition: >
      main.discount_percent <= (
        CASE
          WHEN c.status = 'PLATINUM' AND p.category = 'PREMIUM' THEN 30
          WHEN c.status = 'VIP' AND p.category = 'PREMIUM' THEN 15
          WHEN c.status = 'PLATINUM' THEN 20
          WHEN c.status = 'VIP' THEN 10
          ELSE 5
        END
      )
    related_tables:
      - table: "CUSTOMERS"
        alias: "c"
      - table: "ORDER_LINES"
        alias: "ol"
      - table: "PRODUCTS"
        alias: "p"
    join_conditions:
      - "main.customer_id = c.customer_id"
      - "main.order_id = ol.order_id"
      - "ol.product_id = p.product_id"
```

### Pattern 2: Subquery in Aggregate

**Customer balance = orders - payments:**

```yaml
- type: "MULTI_TABLE_AGGREGATE"
  params:
    target_column: "account_balance"
    aggregate_expr: >
      COALESCE(SUM(o.total_amount), 0) -
      COALESCE(SUM(p.payment_amount), 0)
    related_tables:
      - table: "ORDERS"
        alias: "o"
      - table: "PAYMENTS"
        alias: "p"
    join_conditions:
      - "main.customer_id = o.customer_id"
      - "main.customer_id = p.customer_id"
```

### Pattern 3: Time-Based Aggregation

**YTD sales must match monthly breakdown:**

```yaml
- type: "MULTI_TABLE_AGGREGATE"
  params:
    target_column: "ytd_sales"
    aggregate_expr: >
      SUM(CASE
        WHEN YEAR(sales.sale_date) = YEAR(CURRENT_DATE())
        THEN sales.amount
        ELSE 0
      END)
    related_tables:
      - table: "SALES"
        alias: "sales"
    join_conditions:
      - "main.customer_id = sales.customer_id"
```

---

## ⚡ Performance Considerations

### Indexes Are Critical

For multi-table rules, ensure indexes exist on:
```sql
-- JOIN keys
CREATE INDEX idx_orders_customer ON ORDERS(customer_id);
CREATE INDEX idx_orderlines_order ON ORDER_LINES(order_id);
CREATE INDEX idx_orderlines_product ON ORDER_LINES(product_id);
CREATE INDEX idx_inventory_product ON WAREHOUSE_INVENTORY(product_id);

-- Filter columns
CREATE INDEX idx_orders_status ON ORDERS(status);
CREATE INDEX idx_customers_status ON CUSTOMERS(status);
```

### Expected Performance

| Tables Joined | Rows (Main) | Expected Time | Recommendation |
|--------------|-------------|---------------|----------------|
| 2 tables | < 10K | < 5 sec | ✅ OK |
| 2 tables | < 100K | 10-30 sec | ⚠️ Add indexes |
| 3+ tables | < 10K | 10-20 sec | ⚠️ Test first |
| 3+ tables | > 100K | > 60 sec | ❌ Consider batch processing |

### Optimization Tips

1. **Add WHERE clauses** to limit scope:
   ```yaml
   condition: >
     (main.created_date >= DATEADD(day, -90, CURRENT_DATE())) AND
     (main.status != 'VIP' OR ...)
   ```

2. **Use INNER JOIN** instead of LEFT JOIN when possible

3. **Pre-aggregate** in views:
   ```sql
   CREATE VIEW CUSTOMER_ORDER_TOTALS AS
   SELECT customer_id, SUM(amount) as total_orders
   FROM ORDERS
   GROUP BY customer_id;
   ```

---

## 📋 Summary

You now have **19 total rule types**:

### Column-Level (12)
1. NOT_NULL
2. UNIQUE
3. FUZZY_DUPLICATE
4. MIN_VALUE
5. MAX_VALUE
6. ALLOWED_VALUES
7. MAX_LENGTH
8. PATTERN
9. MAX_AGE_DAYS
10. FOREIGN_KEY
11. LOOKUP
12. (reserved)

### Table-Level (7)
13. COMPOSITE_UNIQUE
14. CROSS_COLUMN_COMPARISON
15. CONDITIONAL_REQUIRED
16. MUTUAL_EXCLUSIVITY
17. CONDITIONAL_VALUE
18. **MULTI_TABLE_AGGREGATE** ⭐ NEW
19. **MULTI_TABLE_CONDITION** ⭐ NEW

---

## ✅ Next Steps

1. **Pull latest code:**
   ```bash
   git pull origin claude/review-rules-field-selection-015E5ng2zxDCy9Eiz3H44iiw
   ```

2. **Test with your scenario**
3. **Report performance and results**

Your data quality tool can now handle **ANY complex business rule** across multiple tables! 🚀
