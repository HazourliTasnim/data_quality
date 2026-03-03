# AI-Powered Rule Creation with Auto Field Detection

## Overview

This feature allows business users to create data quality rules using **pure natural language** without needing to know exact field names. AI automatically:

1. 🔍 Analyzes your table schema
2. 🎯 Identifies which field(s) are relevant to your request
3. 🧠 Determines the appropriate rule type
4. ✅ Generates the complete rule configuration
5. 👤 Shows you what it found for approval before applying

## Why This Matters

**Traditional Approach (Manual Field Selection):**
- User must know: "I want to validate the `customer_email` field"
- User must select the field from dropdown
- User describes: "Must match email pattern"

**AI-Powered Approach (Auto Field Detection):**
- User simply says: "Make sure customer emails are valid"
- AI identifies the `customer_email` field automatically
- AI determines PATTERN rule is appropriate
- AI generates complete validation rule

## How It Works

### Architecture

```
User Natural Language Description
         ↓
    AI Analysis Layer (Snowflake Cortex)
         ↓
    Schema Context + Semantic Understanding
         ↓
    Field Identification + Rule Type Selection
         ↓
    Rule Generation (using existing NL functions)
         ↓
    User Review & Approval
         ↓
    Rule Applied to YAML
```

### AI Decision Process

The AI considers:

1. **Schema Information:**
   - Column names
   - Data types
   - Logical types (email, phone, date, etc.)
   - Existing descriptions

2. **Semantic Understanding:**
   - Keywords in user request ("email", "valid", "required", "unique")
   - Relationships mentioned ("total matches sum", "start before end")
   - Business logic ("active customers", "recent purchases")

3. **Rule Type Selection:**
   - Single field → Column-level rule
   - Multiple fields → Table-level rule
   - Cross-table references → Multi-table rule

## Usage Examples

### Example 1: Email Validation (Single Field)

**User Input:**
```
Make sure customer emails are properly formatted
```

**AI Identifies:**
- **Field:** `customer_email`
- **Rule Type:** `PATTERN`
- **Category:** column-level

**Generated Rule:**
```yaml
columns:
  - name: customer_email
    dq_rules:
      - id: customer_email_pattern
        type: PATTERN
        severity: CRITICAL
        params:
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
        description: "Email must match standard email format"
        lambda_hint: "REGEXP_LIKE(customer_email, '^[a-zA-Z0-9._%+-]+@...')"
```

**Why It Worked:**
- AI identified "email" keyword in request
- Found column with "email" in name
- Logical type confirmed it's an email field
- PATTERN rule is standard for email validation

---

### Example 2: Date Comparison (Multiple Fields)

**User Input:**
```
Start date should always be before end date
```

**AI Identifies:**
- **Fields:** `start_date`, `end_date`
- **Rule Type:** `CROSS_COLUMN_COMPARISON`
- **Category:** table-level

**Generated Rule:**
```yaml
table_rules:
  - id: start_date_end_date_cross_column_comparison
    type: CROSS_COLUMN_COMPARISON
    columns: [start_date, end_date]
    severity: WARNING
    description: "Start date must be before end date"
    lambda_hint: "start_date < end_date OR end_date IS NULL"
```

**Why It Worked:**
- AI identified temporal relationship ("before")
- Found two date-type columns matching "start" and "end"
- CROSS_COLUMN_COMPARISON is appropriate for comparisons

---

### Example 3: Required Fields

**User Input:**
```
Customer name must always be present
```

**AI Identifies:**
- **Field:** `customer_name`
- **Rule Type:** `NOT_NULL`
- **Category:** column-level

**Generated Rule:**
```yaml
columns:
  - name: customer_name
    dq_rules:
      - id: customer_name_not_null
        type: NOT_NULL
        severity: CRITICAL
        params: null
        description: "Customer name is required and cannot be null"
        lambda_hint: "customer_name IS NOT NULL"
```

---

### Example 4: Duplicate Detection

**User Input:**
```
Find customers with similar names that might be duplicates
```

**AI Identifies:**
- **Field:** `customer_name`
- **Rule Type:** `FUZZY_DUPLICATE`
- **Category:** column-level

**Generated Rule:**
```yaml
columns:
  - name: customer_name
    dq_rules:
      - id: customer_name_fuzzy_duplicate
        type: FUZZY_DUPLICATE
        severity: WARNING
        params:
          method: editdistance
          threshold: 0.8
        description: "Detect similar customer names that may be duplicates"
        lambda_hint: "EDITDISTANCE(customer_name, other.customer_name) similarity > 0.8"
```

---

### Example 5: Value Constraints

**User Input:**
```
Order status can only be NEW, PROCESSING, SHIPPED, or DELIVERED
```

**AI Identifies:**
- **Field:** `order_status`
- **Rule Type:** `ALLOWED_VALUES`
- **Category:** column-level

**Generated Rule:**
```yaml
columns:
  - name: order_status
    dq_rules:
      - id: order_status_allowed_values
        type: ALLOWED_VALUES
        severity: CRITICAL
        params:
          allowed: [NEW, PROCESSING, SHIPPED, DELIVERED]
        description: "Order status must be one of the allowed values"
        lambda_hint: "order_status IN ('NEW', 'PROCESSING', 'SHIPPED', 'DELIVERED')"
```

---

### Example 6: Active Customer Validation (Complex Multi-Table)

**User Input:**
```
Customers should only be marked active if they made a purchase in the last 6 months
```

**AI Identifies:**
- **Field:** `active` (in customer table)
- **Rule Type:** `MULTI_TABLE_CONDITION`
- **Category:** table-level

**Generated Rule:**
```yaml
table_rules:
  - id: active_multi_table_condition
    type: MULTI_TABLE_CONDITION
    severity: WARNING
    params:
      filter: "main.active = TRUE"
      condition: >
        COUNT(CASE WHEN s.sale_date >= DATEADD(month, -6, CURRENT_DATE())
              THEN 1 END) > 0
      related_tables:
        - table: "SALES"
          alias: "s"
      join_conditions:
        - "main.customer_id = s.customer_id"
    description: "Active customers must have purchases in last 6 months"
```

**Why It Worked:**
- AI understood "active" implies a boolean/status field
- Recognized temporal business logic ("last 6 months")
- Inferred cross-table relationship with sales data
- Generated appropriate multi-table validation

---

## Supported Rule Types

### Column-Level Rules (Single Field)

| Rule Type | Natural Language Triggers | Examples |
|-----------|---------------------------|----------|
| `NOT_NULL` | "required", "must be present", "cannot be empty" | "Email is required" |
| `UNIQUE` | "unique", "no duplicates", "distinct" | "Customer ID must be unique" |
| `FUZZY_DUPLICATE` | "similar", "fuzzy match", "might be duplicates" | "Find similar customer names" |
| `MIN_VALUE` | "at least", "minimum", "greater than" | "Price must be at least 0" |
| `MAX_VALUE` | "at most", "maximum", "less than" | "Age must be under 120" |
| `ALLOWED_VALUES` | "only be", "must be one of", "valid values" | "Status can only be A, B, or C" |
| `MAX_LENGTH` | "length", "characters", "max length" | "Name cannot exceed 100 characters" |
| `PATTERN` | "format", "pattern", "regex", "valid" | "Phone must match pattern" |
| `MAX_AGE_DAYS` | "recent", "within days", "not older than" | "Data must be within 30 days" |

### Table-Level Rules (Multiple Fields)

| Rule Type | Natural Language Triggers | Examples |
|-----------|---------------------------|----------|
| `COMPOSITE_UNIQUE` | "together unique", "combination unique" | "Customer ID and Order ID together must be unique" |
| `CROSS_COLUMN_COMPARISON` | "before", "after", "greater than", "less than" | "Start date before end date" |
| `CONDITIONAL_REQUIRED` | "if...then required", "when...must have" | "If shipped, tracking number required" |
| `MUTUAL_EXCLUSIVITY` | "only one", "either...or", "mutually exclusive" | "Either email or phone required, not both" |
| `CONDITIONAL_VALUE` | "if...then must be", "when...should be" | "If status is COMPLETE, end_date required" |
| `MULTI_TABLE_AGGREGATE` | "matches sum", "equals total", "aggregate" | "Order total matches line item sum" |
| `MULTI_TABLE_CONDITION` | "based on other table", "if exists in" | "Active customers have recent purchases" |

## User Interface

### Step 1: Describe What You Want

```
┌─────────────────────────────────────────────────────────┐
│ 🤖 AI-Powered Rule Creation (Auto Field Detection)     │
├─────────────────────────────────────────────────────────┤
│                                                          │
│ 💬 What do you want to validate?                        │
│ ┌──────────────────────────────────────────────────┐   │
│ │ Make sure customer emails are properly formatted │   │
│ │                                                    │   │
│ └──────────────────────────────────────────────────┘   │
│                                                          │
│ [ 🔍 Auto-Identify Fields ]                             │
└─────────────────────────────────────────────────────────┘
```

### Step 2: Review AI's Findings

```
┌─────────────────────────────────────────────────────────┐
│ 🎯 AI Identified the Following:                         │
├─────────────────────────────────────────────────────────┤
│                                                          │
│ ┌────────────────┬────────────────┬────────────────┐   │
│ │ Identified     │ Rule Type      │ Category       │   │
│ │ Fields         │                │                │   │
│ ├────────────────┼────────────────┼────────────────┤   │
│ │ customer_email │ PATTERN        │ column-level   │   │
│ └────────────────┴────────────────┴────────────────┘   │
│                                                          │
│ 💡 AI Reasoning: Email field needs format validation    │
│    using regex pattern to ensure valid email structure  │
│                                                          │
│ Generated Rule Description:                              │
│ ┌──────────────────────────────────────────────────┐   │
│ │ Must match email pattern                         │   │
│ └──────────────────────────────────────────────────┘   │
│                                                          │
│ [ ✅ Apply This Rule ]  [ ❌ Try Again ]                │
└─────────────────────────────────────────────────────────┘
```

### Step 3: Rule Applied

```
✅ Rule applied to field(s): customer_email
```

## Technical Implementation

### Function Signature

```python
def auto_identify_and_create_rule(
    conn: SnowflakeConnection,
    yaml_text: str,
    nl_description: str,
    model: str = "mistral-large"
) -> dict:
    """
    Automatically identify relevant field(s) from natural language
    and create appropriate rule.

    Returns:
        dict with:
            - identified_fields: List of field names identified
            - rule_type: Type of rule (column-level or table-level)
            - updated_yaml: Updated YAML with the new rule
            - explanation: Human-readable explanation
    """
```

### AI Prompt Structure

The function uses a two-stage AI process:

**Stage 1: Field Identification**
```python
prompt = """You are a data quality expert. A business user wants to create
a validation rule but doesn't know the exact field names.

Table Schema:
[JSON representation of all columns with types and descriptions]

User Request: "{nl_description}"

Your task:
1. Identify which field(s) are relevant
2. Determine if single-field or multi-field rule
3. Suggest appropriate rule type
4. Provide reasoning

Respond with JSON:
{
  "identified_fields": ["field1", "field2"],
  "rule_category": "column-level" or "table-level",
  "suggested_rule_type": "<RULE_TYPE>",
  "reasoning": "...",
  "nl_rule_description": "Refined description for rule generation"
}
"""
```

**Stage 2: Rule Generation**

Once fields are identified, the function calls the appropriate existing function:
- `add_dq_rule_from_natural_language()` for single-field rules
- `add_table_level_rule_from_natural_language()` for multi-field rules

### Error Handling

The function validates:

1. **Schema Validation:** Identified fields must exist in schema
2. **Field Count Validation:**
   - Column-level rules need exactly 1 field
   - Table-level rules need 2+ fields
3. **JSON Parsing:** AI response must be valid JSON
4. **Required Fields:** All expected fields present in AI response

## Best Practices

### For Business Users

1. **Be Descriptive:**
   - ✅ "Customer emails should be properly formatted"
   - ❌ "Check emails"

2. **Use Business Terms:**
   - ✅ "Active customers must have recent purchases"
   - ❌ "active = 1 if exists in sales where date > now() - 180"

3. **Specify Relationships Clearly:**
   - ✅ "Order total must match the sum of line items"
   - ❌ "Check totals"

4. **Review Before Applying:**
   - Always check the identified fields are correct
   - Verify the rule type makes sense
   - Read the AI's reasoning

### For Developers

1. **Enrich Schema Information:**
   - Add meaningful column descriptions
   - Use logical_type annotations
   - Use clear column naming conventions

2. **Monitor AI Performance:**
   - Track successful vs failed identifications
   - Review cases where AI misidentified fields
   - Improve schema descriptions based on failures

3. **Provide Fallback:**
   - Keep manual field selection available
   - Allow users to edit AI-generated rules
   - Provide clear error messages

## Limitations

1. **Schema Knowledge Required:**
   - AI can only identify fields that exist in the schema
   - Cannot create new fields or suggest schema changes

2. **Single Table Context:**
   - Current implementation focuses on single table
   - Multi-table rules require tables to be referenced in schema

3. **AI Model Constraints:**
   - Depends on Snowflake Cortex availability
   - Mistral-large model must be accessible
   - Subject to LLM token limits

4. **Ambiguity Handling:**
   - If multiple fields could match, AI picks most likely
   - User should review and reject if incorrect

## Future Enhancements

### Planned Features

1. **Multi-Table Auto-Detection:**
   - Analyze foreign key relationships
   - Automatically identify related tables
   - Suggest cross-table validation rules

2. **Rule Templates:**
   - Learn from user approvals/rejections
   - Build organization-specific rule patterns
   - Suggest common rules based on data type

3. **Bulk Rule Creation:**
   - "Apply standard email validation to all email fields"
   - "Check all date pairs for logical ordering"
   - "Ensure all numeric fields are non-negative"

4. **Interactive Refinement:**
   - If AI is uncertain, ask clarifying questions
   - Allow user to choose between multiple interpretations
   - Provide confidence scores for identifications

5. **Rule Conflict Detection:**
   - Warn if new rule conflicts with existing rules
   - Suggest rule consolidation
   - Detect redundant rules

## Comparison: Manual vs AI-Powered

| Aspect | Manual Selection | AI-Powered |
|--------|------------------|------------|
| **Field Knowledge** | Required | Not Required |
| **Speed** | 3-4 clicks + typing | 1 text input |
| **Learning Curve** | Steep for non-technical | Minimal |
| **Accuracy** | Depends on user | Depends on AI + review |
| **Flexibility** | Full control | AI interpretation |
| **Best For** | Technical users, precise control | Business users, quick setup |
| **Error Risk** | Wrong field selection | Wrong field identification |

## Real-World Use Cases

### Use Case 1: Non-Technical Data Steward

**Scenario:** Marketing manager wants to ensure CRM data quality

**Challenge:** Knows business rules but not SQL or field names

**Solution:**
```
"Customer emails must be valid"
"Phone numbers should follow US format"
"No customers with same name and address"
```

**Result:** 3 validation rules created in minutes without technical knowledge

---

### Use Case 2: Rapid Prototyping

**Scenario:** Data engineer setting up new data pipeline

**Challenge:** Need quick validation rules, will refine later

**Solution:**
```
"Order dates must be recent"
"Prices cannot be negative"
"Status must be valid"
```

**Result:** Basic validation in place immediately, refine rules as needed

---

### Use Case 3: Business Logic Documentation

**Scenario:** Analyst documenting existing business rules

**Challenge:** Business rules scattered across documentation

**Solution:** Convert documentation to validation rules:
```
From: "Active customers are defined as customers with purchases
       in the last 180 days"
To: "Customers marked active must have purchases within 6 months"
```

**Result:** Business logic now enforced as validation rules

## Troubleshooting

### Issue: AI Identifies Wrong Field

**Symptoms:** AI picks a field that doesn't match your intent

**Solutions:**
1. Click "Try Again" and rephrase your request
2. Use more specific terms that match field names
3. Fall back to manual field selection
4. Update column descriptions to improve future matches

**Example:**
```
Original: "Check emails"
Improved: "Validate customer contact email format"
```

---

### Issue: AI Suggests Wrong Rule Type

**Symptoms:** Rule type doesn't match your validation goal

**Solutions:**
1. Be more explicit about what you're checking
2. Use rule-type-specific keywords
3. Review the generated rule before applying
4. Manually select fields and rule type

**Example:**
```
Original: "Check start and end dates"
Improved: "Start date must be before end date"  (triggers CROSS_COLUMN_COMPARISON)
```

---

### Issue: AI Cannot Find Relevant Field

**Symptoms:** "AI could not identify any relevant fields"

**Solutions:**
1. Check if the field actually exists in the table
2. Use terms closer to the actual field name
3. Add descriptions to your columns
4. Use manual field selection

---

### Issue: Multi-Table Rules Not Working

**Symptoms:** AI doesn't recognize cross-table relationships

**Current Limitation:** Multi-table rules require explicit table references

**Workaround:**
```
Include table name: "Customer active status should match sales transactions"
```

**Future Enhancement:** Auto-detect foreign key relationships

## Performance Considerations

### AI Call Overhead

- **Single AI Call:** ~1-3 seconds for field identification
- **Second AI Call:** ~1-2 seconds for rule generation
- **Total Time:** 2-5 seconds end-to-end

### Cost Optimization

Snowflake Cortex charges per token:
- **Schema Size:** Larger schemas = more tokens
- **Model Choice:** mistral-large vs mistral-7b
- **Caching:** Consider caching schema representations

### Optimization Strategies

1. **Minimize Schema Size:** Only include relevant columns
2. **Use Smaller Models:** For simple identifications, use mistral-7b
3. **Batch Requests:** Group similar rule requests
4. **Cache Results:** Store common field→rule mappings

## Security & Governance

### Data Privacy

- ✅ No actual data sent to AI (only schema metadata)
- ✅ Column names and types are metadata, not PII
- ✅ Descriptions should not contain sensitive information

### Access Control

- Users need appropriate Snowflake permissions
- Cortex functions must be accessible
- Rule creation follows existing RBAC

### Audit Trail

All rule creations logged with:
- User who created rule
- Original natural language input
- AI-identified fields
- Generated rule configuration
- Timestamp

## Conclusion

AI-Powered Rule Creation with Auto Field Detection democratizes data quality validation by:

- 🎯 **Removing technical barriers** for business users
- ⚡ **Accelerating rule creation** from minutes to seconds
- 🧠 **Leveraging semantic understanding** of your data
- ✅ **Maintaining accuracy** through review process
- 🔄 **Complementing manual methods** rather than replacing them

**Key Takeaway:** Business users can now describe *what* they want to validate without needing to know *how* or *where* it's stored.
