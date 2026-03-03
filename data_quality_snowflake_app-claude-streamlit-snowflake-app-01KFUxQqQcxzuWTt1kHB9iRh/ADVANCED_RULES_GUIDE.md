# Advanced Data Quality Rules Guide

**Version:** 1.1
**Last Updated:** 2025-11-23

---

## 🎯 Advanced Scenarios Covered

This guide addresses real-world data quality challenges:

1. **Fuzzy Deduplication** - Find similar but not identical values
2. **Cross-Table Validation** - Validate data across multiple tables
3. **Complex Business Rules** - Multi-table dependencies

---

## 🔍 Scenario 1: Fuzzy Deduplication

### Problem: Same Customer, Different Spellings

**Example Data:**
```sql
CREATE TABLE CUSTOMERS (
    customer_id NUMBER,
    customer_name VARCHAR,
    email VARCHAR
);

INSERT INTO CUSTOMERS VALUES
    (1, 'John Smith', 'john@email.com'),
    (2, 'Jon Smith', 'jon@email.com'),       -- Similar name!
    (3, 'Jonathan Smith', 'j.smith@email.com'),  -- Similar name!
    (4, 'Jane Doe', 'jane@email.com'),
    (5, 'Jayne Doe', 'jayne@email.com');     -- Similar name!
```

**Problem:**
- Customer ID 1, 2, 3 are probably the same person (John/Jon/Jonathan Smith)
- Customer ID 4, 5 are probably the same person (Jane/Jayne Doe)
- Standard UNIQUE rule won't catch this!

### Solution: FUZZY_DUPLICATE Rule

#### Method 1: Edit Distance (Levenshtein)

Finds strings that are similar by counting character changes needed.

**YAML Definition:**
```yaml
semantic_view:
  name: "customer_view"
  source:
    database: "MYDB"
    schema: "SALES"
    table: "CUSTOMERS"
  columns:
    - name: "customer_name"
      data_type: "VARCHAR"
      dq_rules:
        - id: "customer_name_fuzzy_dup"
          type: "FUZZY_DUPLICATE"
          severity: "WARNING"
          description: "Find similar customer names (possible duplicates)"
          params:
            method: "editdistance"      # Use Levenshtein distance
            threshold: 0.8              # 80% similar
```

**How it works:**
```
"John Smith" vs "Jon Smith"
- Edit distance: 1 (remove 'h')
- Similarity: 1 - (1/10) = 0.9 = 90% similar ✅ FOUND

"John Smith" vs "Jonathan Smith"
- Edit distance: 4 (add 'a','t','h','a','n')
- Similarity: 1 - (4/14) = 0.71 = 71% similar ❌ Not similar enough
```

**Adjust threshold:**
- `threshold: 0.9` → Very strict (only catches typos)
- `threshold: 0.7` → More lenient (catches variations)
- `threshold: 0.5` → Very loose (may have false positives)

**SQL Generated:**
```sql
WITH pairs AS (
    SELECT
        a.customer_name as value_a,
        b.customer_name as value_b,
        EDITDISTANCE(LOWER(a.customer_name), LOWER(b.customer_name)) as distance,
        GREATEST(LENGTH(a.customer_name), LENGTH(b.customer_name)) as max_len,
        1.0 - (EDITDISTANCE(LOWER(a.customer_name), LOWER(b.customer_name)) /
               GREATEST(LENGTH(a.customer_name), LENGTH(b.customer_name))) as similarity,
        a.*
    FROM CUSTOMERS a
    JOIN CUSTOMERS b
        ON a.customer_name != b.customer_name
        AND a.customer_name IS NOT NULL
        AND b.customer_name IS NOT NULL
    WHERE EDITDISTANCE(LOWER(a.customer_name), LOWER(b.customer_name)) <=
          GREATEST(LENGTH(a.customer_name), LENGTH(b.customer_name)) * 0.2
)
SELECT DISTINCT * FROM pairs
WHERE similarity >= 0.8
```

#### Method 2: Phonetic Matching (SOUNDEX)

Finds names that **sound similar** when spoken.

**YAML Definition:**
```yaml
- id: "customer_name_soundex"
  type: "FUZZY_DUPLICATE"
  severity: "WARNING"
  description: "Find names that sound similar"
  params:
    method: "soundex"
```

**How it works:**
```
SOUNDEX("John Smith") = "J525"
SOUNDEX("Jon Smith")  = "J525"  ✅ Same code!
SOUNDEX("Jane Doe")   = "J530"
SOUNDEX("Jayne Doe")  = "J530"  ✅ Same code!
```

**Great for:**
- Name variations (John/Jon, Catherine/Katherine)
- Phone transcription errors
- Different language spellings

**SQL Generated:**
```sql
WITH soundex_groups AS (
    SELECT
        SOUNDEX(customer_name) as soundex_code,
        customer_name,
        COUNT(*) OVER (PARTITION BY SOUNDEX(customer_name)) as similar_count
    FROM CUSTOMERS
    WHERE customer_name IS NOT NULL
)
SELECT *
FROM soundex_groups
WHERE similar_count > 1
```

#### Method 3: Normalized Matching

Removes spaces, special characters, and case to find duplicates.

**YAML Definition:**
```yaml
- id: "email_normalized_dup"
  type: "FUZZY_DUPLICATE"
  severity: "CRITICAL"
  description: "Find emails that are the same when normalized"
  params:
    method: "normalized"
```

**How it works:**
```
Original: "john.smith@email.com"
Normalized: "johnsmithemailcom"

Original: "John Smith@email.com"
Normalized: "johnsmithemailcom"  ✅ Same!

Original: "JOHN_SMITH@EMAIL.COM"
Normalized: "johnsmithemailcom"  ✅ Same!
```

**Great for:**
- Email addresses with spacing variations
- Phone numbers with different formats (+1-555-1234 vs 5551234)
- Product codes with inconsistent formatting

**SQL Generated:**
```sql
WITH normalized AS (
    SELECT
        email,
        TRIM(LOWER(REGEXP_REPLACE(email, '\s+', ''))) as normalized_value,
        COUNT(*) OVER (PARTITION BY TRIM(LOWER(REGEXP_REPLACE(email, '\s+', '')))) as dup_count
    FROM CUSTOMERS
    WHERE email IS NOT NULL
)
SELECT *
FROM normalized
WHERE dup_count > 1
```

### Real-World Use Cases

#### Use Case 1: Customer Master Data Cleanup

**Problem:** Multiple customer records for same person

```yaml
columns:
  - name: "customer_name"
    dq_rules:
      - type: "FUZZY_DUPLICATE"
        severity: "WARNING"
        params:
          method: "editdistance"
          threshold: 0.85

  - name: "email"
    dq_rules:
      - type: "FUZZY_DUPLICATE"
        severity: "CRITICAL"
        params:
          method: "normalized"
```

**Results:**
- Find: "john.smith@gmail.com" vs "johnsmith@gmail.com"
- Find: "Robert Johnson" vs "Bob Johnson"
- Deduplicate before AI training!

#### Use Case 2: Product Catalog Normalization

**Problem:** Same product, different names

```sql
-- Products table
(101, 'iPhone 15 Pro Max')
(102, 'iphone15promax')      -- No spaces
(103, 'I Phone 15 Pro Max')  -- Extra space
```

```yaml
columns:
  - name: "product_name"
    dq_rules:
      - type: "FUZZY_DUPLICATE"
        severity: "WARNING"
        params:
          method: "normalized"
```

---

## 🔗 Scenario 2: Cross-Table Validation (Foreign Keys)

### Problem: Invalid References

**Example Data:**
```sql
CREATE TABLE CUSTOMERS (
    customer_id NUMBER PRIMARY KEY,
    customer_name VARCHAR
);

CREATE TABLE ORDERS (
    order_id NUMBER PRIMARY KEY,
    customer_id NUMBER,    -- Should reference CUSTOMERS
    amount NUMBER
);

-- Good data
INSERT INTO CUSTOMERS VALUES (1, 'John'), (2, 'Jane');

-- Problem: Order references non-existent customer!
INSERT INTO ORDERS VALUES
    (1001, 1, 100.00),     -- OK: customer 1 exists
    (1002, 999, 50.00);    -- ❌ ERROR: customer 999 doesn't exist!
```

**Problem:**
- Order 1002 references customer_id=999 which doesn't exist in CUSTOMERS table
- This is orphaned data (referential integrity violation)

### Solution: FOREIGN_KEY Rule

**YAML Definition:**
```yaml
semantic_view:
  name: "order_view"
  source:
    database: "MYDB"
    schema: "SALES"
    table: "ORDERS"
  columns:
    - name: "customer_id"
      data_type: "NUMBER"
      dq_rules:
        - id: "customer_id_fk"
          type: "FOREIGN_KEY"
          severity: "CRITICAL"
          description: "Customer ID must exist in CUSTOMERS table"
          params:
            ref_database: "MYDB"      # Reference table database
            ref_schema: "SALES"        # Reference table schema
            ref_table: "CUSTOMERS"     # Reference table name
            ref_column: "customer_id"  # Reference column name
```

**SQL Generated:**
```sql
SELECT t.*
FROM MYDB.SALES.ORDERS t
LEFT JOIN MYDB.SALES.CUSTOMERS ref
    ON t.customer_id = ref.customer_id
WHERE t.customer_id IS NOT NULL
    AND ref.customer_id IS NULL
```

**Results:**
```
order_id | customer_id | amount
---------|-------------|-------
1002     | 999         | 50.00   ← VIOLATION: Customer 999 not found!
```

### Advanced Cross-Table Scenarios

#### Scenario A: Multi-Table Chain

Validate that a chain of references is intact:

```
ORDERS.customer_id → CUSTOMERS.customer_id
ORDERS.product_id → PRODUCTS.product_id
ORDERS.store_id → STORES.store_id
```

**YAML Definition:**
```yaml
columns:
  - name: "customer_id"
    dq_rules:
      - type: "FOREIGN_KEY"
        severity: "CRITICAL"
        params:
          ref_table: "CUSTOMERS"
          ref_column: "customer_id"

  - name: "product_id"
    dq_rules:
      - type: "FOREIGN_KEY"
        severity: "CRITICAL"
        params:
          ref_table: "PRODUCTS"
          ref_column: "product_id"

  - name: "store_id"
    dq_rules:
      - type: "FOREIGN_KEY"
        severity: "WARNING"
        params:
          ref_table: "STORES"
          ref_column: "store_id"
```

#### Scenario B: Cross-Database References

Validate references across different databases:

```yaml
- name: "employee_id"
  dq_rules:
    - type: "FOREIGN_KEY"
      severity: "CRITICAL"
      params:
        ref_database: "HR_DB"        # Different database!
        ref_schema: "EMPLOYEES"
        ref_table: "EMPLOYEE_MASTER"
        ref_column: "emp_id"
```

#### Scenario C: Composite Foreign Keys

**Future Enhancement (v1.2):** Validate multi-column foreign keys

```yaml
# Coming in v1.2
table_rules:
  - type: "COMPOSITE_FOREIGN_KEY"
    columns: ["order_id", "line_number"]
    severity: "CRITICAL"
    params:
      ref_table: "ORDER_LINES"
      ref_columns: ["order_id", "line_num"]
```

---

## 🎯 Scenario 3: Complex Multi-Table Business Rules

### Problem: Dependent Columns Across Tables

**Example:** Order total should match sum of order lines

```sql
-- ORDERS table
order_id | customer_id | total_amount
---------|-------------|-------------
1001     | 1           | 150.00

-- ORDER_LINES table
order_id | line_num | product | price
---------|----------|---------|-------
1001     | 1        | Widget  | 50.00
1001     | 2        | Gadget  | 100.00
                              --------
                   SUM:       150.00  ✅ Matches!
```

**But what if:**
```sql
-- ORDER_LINES changed but ORDERS not updated
order_id | line_num | product | price
---------|----------|---------|-------
1001     | 1        | Widget  | 50.00
1001     | 2        | Gadget  | 100.00
1001     | 3        | Tool    | 25.00   ← New line added!
                              --------
                   SUM:       175.00  ❌ Doesn't match 150.00!
```

### Solution: Custom SQL Rule (Future v1.2)

**YAML Definition:**
```yaml
# Coming in v1.2
table_rules:
  - type: "CUSTOM_SQL"
    severity: "CRITICAL"
    description: "Order total must equal sum of line items"
    sql_check: |
      SELECT o.order_id, o.total_amount, SUM(ol.price) as calculated_total
      FROM ORDERS o
      LEFT JOIN ORDER_LINES ol ON o.order_id = ol.order_id
      GROUP BY o.order_id, o.total_amount
      HAVING o.total_amount != SUM(ol.price)
```

### Workaround for v1.1: Use FOREIGN_KEY Creatively

You can validate existence across tables today:

```yaml
# Validate that every order has at least one line item
- name: "order_id"
  dq_rules:
    - type: "FOREIGN_KEY"
      severity: "WARNING"
      description: "Order must have line items"
      params:
        ref_table: "ORDER_LINES"
        ref_column: "order_id"
```

---

## 📊 Complete Example: E-Commerce Data Quality

### Database Schema

```sql
-- Customer table
CREATE TABLE CUSTOMERS (
    customer_id NUMBER PRIMARY KEY,
    customer_name VARCHAR,
    email VARCHAR,
    created_date DATE
);

-- Products table
CREATE TABLE PRODUCTS (
    product_id NUMBER PRIMARY KEY,
    product_name VARCHAR,
    category VARCHAR,
    price NUMBER
);

-- Orders table
CREATE TABLE ORDERS (
    order_id NUMBER PRIMARY KEY,
    customer_id NUMBER,      -- FK to CUSTOMERS
    order_date DATE,
    total_amount NUMBER
);

-- Order lines table
CREATE TABLE ORDER_LINES (
    order_id NUMBER,         -- FK to ORDERS
    line_number NUMBER,
    product_id NUMBER,       -- FK to PRODUCTS
    quantity NUMBER,
    price NUMBER,
    PRIMARY KEY (order_id, line_number)
);
```

### Comprehensive YAML with Advanced Rules

```yaml
semantic_view:
  name: "ecommerce_quality_check"
  version: 1

  # ORDERS Table Validation
  source:
    database: "ECOMMERCE"
    schema: "SALES"
    table: "ORDERS"

  columns:
    - name: "customer_id"
      data_type: "NUMBER"
      description: "Reference to customer who placed order"
      dq_rules:
        # Check referential integrity
        - id: "customer_exists"
          type: "FOREIGN_KEY"
          severity: "CRITICAL"
          description: "Customer must exist in CUSTOMERS table"
          params:
            ref_table: "CUSTOMERS"
            ref_column: "customer_id"

        # Check not null
        - id: "customer_required"
          type: "NOT_NULL"
          severity: "CRITICAL"
          description: "Every order must have a customer"

    - name: "total_amount"
      data_type: "NUMBER"
      description: "Total order amount"
      dq_rules:
        - id: "positive_amount"
          type: "MIN_VALUE"
          severity: "CRITICAL"
          description: "Order total must be positive"
          params:
            min: 0.01

        - id: "reasonable_amount"
          type: "MAX_VALUE"
          severity: "WARNING"
          description: "Order total seems suspiciously high"
          params:
            max: 100000

    - name: "order_date"
      data_type: "DATE"
      description: "Date order was placed"
      dq_rules:
        - id: "not_future_date"
          type: "MAX_VALUE"
          severity: "CRITICAL"
          description: "Order date cannot be in the future"
          params:
            max: "CURRENT_DATE()"

        - id: "not_too_old"
          type: "MAX_AGE_DAYS"
          severity: "INFO"
          description: "Order is very old"
          params:
            max_age_days: 3650  # 10 years

# CUSTOMERS Table Validation
---
semantic_view:
  name: "customer_quality_check"
  source:
    database: "ECOMMERCE"
    schema: "CUSTOMERS"
    table: "CUSTOMERS"

  columns:
    - name: "customer_name"
      data_type: "VARCHAR"
      dq_rules:
        # Find duplicate/similar customers
        - id: "duplicate_customers_fuzzy"
          type: "FUZZY_DUPLICATE"
          severity: "WARNING"
          description: "Find customers with similar names (possible duplicates)"
          params:
            method: "editdistance"
            threshold: 0.85

        - id: "duplicate_customers_soundex"
          type: "FUZZY_DUPLICATE"
          severity: "INFO"
          description: "Find customers with phonetically similar names"
          params:
            method: "soundex"

    - name: "email"
      data_type: "VARCHAR"
      dq_rules:
        # Email must be unique
        - id: "unique_email"
          type: "UNIQUE"
          severity: "CRITICAL"
          description: "Each email must be unique"

        # Find normalized duplicates
        - id: "normalized_email_dup"
          type: "FUZZY_DUPLICATE"
          severity: "WARNING"
          description: "Find emails that are identical when normalized"
          params:
            method: "normalized"

        # Email format validation
        - id: "valid_email_format"
          type: "PATTERN"
          severity: "CRITICAL"
          description: "Email must be valid format"
          params:
            pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

# ORDER_LINES Table Validation
---
semantic_view:
  name: "order_lines_quality_check"
  source:
    database: "ECOMMERCE"
    schema: "SALES"
    table: "ORDER_LINES"

  columns:
    - name: "order_id"
      data_type: "NUMBER"
      dq_rules:
        - id: "order_exists"
          type: "FOREIGN_KEY"
          severity: "CRITICAL"
          description: "Order must exist in ORDERS table"
          params:
            ref_table: "ORDERS"
            ref_column: "order_id"

    - name: "product_id"
      data_type: "NUMBER"
      dq_rules:
        - id: "product_exists"
          type: "FOREIGN_KEY"
          severity: "CRITICAL"
          description: "Product must exist in PRODUCTS table"
          params:
            ref_table: "PRODUCTS"
            ref_column: "product_id"

    - name: "quantity"
      data_type: "NUMBER"
      dq_rules:
        - id: "positive_quantity"
          type: "MIN_VALUE"
          severity: "CRITICAL"
          description: "Quantity must be at least 1"
          params:
            min: 1

  table_rules:
    # Composite unique key
    - id: "unique_order_line"
      type: "COMPOSITE_UNIQUE"
      columns: ["order_id", "line_number"]
      severity: "CRITICAL"
      description: "Each order can only have one line with a given line number"
```

---

## 🚀 How to Use These Rules

### Step 1: Update Your YAML

Add the new rule types to your semantic YAML:

```yaml
dq_rules:
  - id: "my_fuzzy_rule"
    type: "FUZZY_DUPLICATE"
    severity: "WARNING"
    params:
      method: "editdistance"
      threshold: 0.8
```

### Step 2: Execute Rules

Use the "Validate & Export" tab in the app:

1. Load your YAML
2. Click "▶️ Run All Rules"
3. View results showing fuzzy duplicates
4. Export results to CSV

### Step 3: Review Violations

The results will show:
- Which records are similar
- Similarity score
- SQL query used
- Sample violations

---

## 📈 Performance Considerations

### FUZZY_DUPLICATE Performance

**Warning:** Fuzzy matching can be SLOW on large tables!

| Table Size | Method | Expected Time | Recommendation |
|------------|--------|---------------|----------------|
| < 1,000 rows | editdistance | < 5 seconds | OK to use |
| 1,000 - 10,000 | editdistance | 30-60 seconds | Use with caution |
| > 10,000 rows | editdistance | > 2 minutes | Use normalized instead |
| Any size | soundex | Fast | Good for large tables |
| Any size | normalized | Fast | Best for large tables |

**Optimization Tips:**

1. **Use WHERE filters** to reduce search space:
   ```sql
   -- Only check recent records
   WHERE created_date >= DATEADD(day, -30, CURRENT_DATE())
   ```

2. **Use normalized method first**, then editdistance on suspects:
   - Step 1: Find candidates with normalized matching (fast)
   - Step 2: Refine with editdistance (slower but accurate)

3. **Sample large tables:**
   ```sql
   -- Only check 10% of records
   WHERE MOD(customer_id, 10) = 0
   ```

### FOREIGN_KEY Performance

Generally fast, but consider:

1. **Ensure indexes exist** on both tables:
   ```sql
   CREATE INDEX idx_orders_customer ON ORDERS(customer_id);
   CREATE INDEX idx_customers_id ON CUSTOMERS(customer_id);
   ```

2. **Use clustering** for very large tables (Snowflake feature)

---

## 🔮 Coming in v1.2: Multi-Table Support

These features are planned:

1. **CROSS_TABLE_AGGREGATE**
   ```yaml
   - type: "CROSS_TABLE_AGGREGATE"
     description: "Order total = sum of lines"
     main_table: "ORDERS"
     related_table: "ORDER_LINES"
     condition: "total_amount = SUM(line_amount)"
   ```

2. **COMPOSITE_FOREIGN_KEY**
   ```yaml
   - type: "COMPOSITE_FOREIGN_KEY"
     columns: ["order_id", "customer_id"]
     ref_columns: ["id", "customer_id"]
   ```

3. **CUSTOM_SQL**
   ```yaml
   - type: "CUSTOM_SQL"
     sql: "SELECT * FROM ... WHERE <custom logic>"
   ```

---

## 📚 Summary

| Rule Type | Use Case | Performance | Complexity |
|-----------|----------|-------------|------------|
| **FUZZY_DUPLICATE (editdistance)** | Find similar strings | Slow | Medium |
| **FUZZY_DUPLICATE (soundex)** | Find names that sound alike | Fast | Low |
| **FUZZY_DUPLICATE (normalized)** | Find formatting variations | Fast | Low |
| **FOREIGN_KEY** | Validate cross-table references | Fast* | Low |
| **COMPOSITE_UNIQUE** | Multi-column uniqueness | Fast | Low |
| **CROSS_COLUMN_COMPARISON** | Compare two columns | Fast | Low |

*Fast if indexes exist

---

**Next:** Test these rules with your data and report any issues!
