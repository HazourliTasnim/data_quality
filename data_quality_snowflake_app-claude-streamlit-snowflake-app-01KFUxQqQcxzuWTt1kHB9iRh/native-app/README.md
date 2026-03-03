# 🎯 Data Quality AI - Snowflake Native App

**AI-Powered Data Quality Rules for Everyone**

Define data quality rules in plain English. Our AI translates them into technical SQL validation rules automatically. Built for business users, loved by data teams.

## ✨ Key Features

### 🤖 **AI-Powered Rule Generation**
- Describe rules in natural language: *"customer_id must always be present"*
- AI translates to technical rules automatically
- No SQL knowledge required

### 🔗 **Hybrid Rule System**
- **Column-level rules**: NOT_NULL, UNIQUE, PATTERN, MIN/MAX values
- **Cross-column rules**: Composite keys, conditional validation, date comparisons
- Industry-first hybrid approach for comprehensive data quality

### 🚀 **Snowflake Native**
- Runs directly in your Snowflake environment
- No data movement or security concerns
- Uses Snowflake Cortex AI (included)
- Leverages your existing compute resources

### 👥 **Built for Non-Technical Users**
- Visual rule builder interface
- Plain English descriptions
- Interactive validation reports
- Export to YAML for version control

## 🎯 Use Cases

### **Data Engineering Teams**
- Automate data quality checks
- Reduce manual SQL writing by 80%
- Standardize validation across pipelines

### **Business Analysts**
- Define business rules yourself
- No waiting for engineering resources
- Immediate validation feedback

### **Data Governance**
- Centralized rule management
- Audit trail of all quality checks
- Compliance-ready documentation

## 📋 Common Data Quality Rules

**Single Column Rules:**
- ✅ NOT_NULL: `customer_id must always be present`
- ✅ UNIQUE: `email addresses must be unique`
- ✅ PATTERN: `phone numbers must match format (xxx) xxx-xxxx`
- ✅ ALLOWED_VALUES: `status can only be ACTIVE, INACTIVE, or PENDING`
- ✅ MIN/MAX_VALUE: `revenue must be greater than 0`

**Cross-Column Rules:**
- ✅ COMPOSITE_UNIQUE: `customer_id and order_id together must be unique`
- ✅ CROSS_COLUMN_COMPARISON: `start_date must be before end_date`
- ✅ CONDITIONAL_REQUIRED: `if status=COMPLETED then completion_date required`
- ✅ MUTUAL_EXCLUSIVITY: `only one of email or phone can be filled`

## 🚀 Quick Start

### 1. Install the App
```sql
-- From Snowflake Marketplace
USE ROLE ACCOUNTADMIN;
CREATE APPLICATION DATA_QUALITY_AI
  FROM APPLICATION PACKAGE DATA_QUALITY_AI_PACKAGE;
```

### 2. Grant Permissions
```sql
-- Grant access to your data
GRANT USAGE ON DATABASE YOUR_DATABASE TO APPLICATION DATA_QUALITY_AI;
GRANT USAGE ON SCHEMA YOUR_SCHEMA TO APPLICATION DATA_QUALITY_AI;
```

### 3. Launch the Interface
Navigate to: `Apps > Data Quality AI > Streamlit`

### 4. Start Building Rules
1. Select your table
2. Choose column or cross-column rule
3. Describe the rule in plain English
4. Let AI generate the technical rule
5. Review and save

## 💡 Example Workflow

**Business User Says:**
> "Customer emails must be unique and follow valid email format. Order amounts must be positive. If order status is SHIPPED, the shipping_date must be filled in."

**Data Quality AI Creates:**
```yaml
semantic_view:
  columns:
    - name: email
      dq_rules:
        - type: UNIQUE
          severity: CRITICAL
          description: "Customer emails must be unique"
        - type: PATTERN
          severity: WARNING
          description: "Must follow valid email format"
          params:
            pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

    - name: order_amount
      dq_rules:
        - type: MIN_VALUE
          severity: CRITICAL
          description: "Order amounts must be positive"
          params:
            min: 0

  table_rules:
    - type: CONDITIONAL_REQUIRED
      columns: [status, shipping_date]
      severity: WARNING
      description: "If order status is SHIPPED, shipping_date must be filled"
      lambda_hint: "status != 'SHIPPED' OR shipping_date IS NOT NULL"
```

## 🔧 Requirements

- Snowflake account (any edition)
- ACCOUNTADMIN role for installation
- Snowflake Cortex AI enabled (included in most regions)
- Warehouse for compute (uses your existing warehouse)

## 📊 Pricing

**Starter**: $500/month
- Up to 25 tables
- 5 users
- 500 AI rule generations/month
- Email support

**Professional**: $1,500/month ⭐ Most Popular
- Up to 150 tables
- 15 users
- 2,500 AI rule generations/month
- Priority support
- Advanced analytics

**Enterprise**: Custom pricing
- Unlimited tables & users
- Unlimited AI generations
- Dedicated support
- Custom integrations
- SLA guarantees

## 🤝 Support

- **Documentation**: [View full docs](#)
- **Support Email**: support@dataqualityai.com
- **Community Slack**: [Join here](#)
- **Video Tutorials**: [Watch demos](#)

## 🔒 Security & Privacy

- ✅ Runs entirely in your Snowflake environment
- ✅ No data leaves your account
- ✅ No external API calls (except Snowflake Cortex)
- ✅ SOC 2 Type II compliant
- ✅ GDPR & HIPAA ready

## 📈 What Customers Say

> "Cut our data quality rule creation time from days to minutes. The AI translation is incredibly accurate."
> — Head of Data, Fortune 500 Retailer

> "Finally, our business analysts can define their own quality checks without bugging engineering."
> — VP Engineering, SaaS Company

> "The cross-column rules feature is a game-changer. No other tool does this."
> — Data Architect, Financial Services

## 🎓 Learn More

- [Video Demo](#) (5 min)
- [Documentation](#)
- [Blog: Why We Built This](#)
- [Case Studies](#)

## 📝 Version History

**v1.0.0** - Initial Release
- AI-powered rule generation
- Hybrid column/table-level rules
- Snowflake Cortex integration
- Visual rule builder
- YAML export

---

**Built by data teams, for data teams. Powered by Snowflake Cortex AI.**

[Get Started](#) | [Contact Sales](#) | [View Pricing](#)
