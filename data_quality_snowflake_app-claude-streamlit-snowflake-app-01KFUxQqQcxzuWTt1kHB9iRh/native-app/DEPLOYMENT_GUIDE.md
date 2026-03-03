# 🚀 Snowflake Native App Deployment Guide

## 📋 Prerequisites

Before deploying, ensure you have:
- ✅ Snowflake account with ACCOUNTADMIN role
- ✅ SnowSQL CLI installed (for file uploads)
- ✅ Files in this directory ready to deploy

## 🗂️ File Structure

```
native-app/
├── manifest.yml                # App metadata and configuration
├── setup.sql                   # Installation script
├── README.md                   # Marketplace listing description
├── streamlit_app.py           # Main Streamlit application
├── snowflake_utils.py         # Snowflake utility functions
├── semantic_yaml_spec.py      # YAML generation logic
├── doc_snippets.py            # Documentation helpers
├── requirements.txt           # Python dependencies
├── deploy.sql                 # Deployment script
└── DEPLOYMENT_GUIDE.md        # This file
```

## 🎯 Deployment Steps

### Step 1: Test Locally First (Optional but Recommended)

```bash
# Run locally to test functionality
cd /home/user/data_quality_snowflake_app/native-app
streamlit run streamlit_app.py
```

**Test checklist:**
- [ ] App loads without errors
- [ ] Can connect to Snowflake
- [ ] Can select database/schema/table
- [ ] Can generate YAML with AI
- [ ] Can add single-column rules
- [ ] Can add cross-column rules
- [ ] Rules display correctly

### Step 2: Prepare for Deployment

1. **Review manifest.yml**
   - Check version number
   - Verify privileges needed
   - Update labels/comments if needed

2. **Review setup.sql**
   - Verify table schemas
   - Check stored procedures
   - Confirm grants are correct

3. **Update README.md**
   - Add your contact info
   - Update pricing if needed
   - Add real customer testimonials (when available)

### Step 3: Deploy to Snowflake

#### Option A: Using SnowSQL (Recommended)

```bash
# 1. Connect to Snowflake
snowsql -a <your-account> -u <your-username>

# 2. Run deployment script
!source deploy.sql

# 3. Upload files (run from your terminal, not in SnowSQL)
cd /home/user/data_quality_snowflake_app/native-app

snowsql -a <account> -u <user> -q "
PUT file://manifest.yml @DATA_QUALITY_AI_PACKAGE.STAGE_CONTENT.APP_STAGE/v1_0_0/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://setup.sql @DATA_QUALITY_AI_PACKAGE.STAGE_CONTENT.APP_STAGE/v1_0_0/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://README.md @DATA_QUALITY_AI_PACKAGE.STAGE_CONTENT.APP_STAGE/v1_0_0/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://streamlit_app.py @DATA_QUALITY_AI_PACKAGE.STAGE_CONTENT.APP_STAGE/v1_0_0/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://snowflake_utils.py @DATA_QUALITY_AI_PACKAGE.STAGE_CONTENT.APP_STAGE/v1_0_0/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://semantic_yaml_spec.py @DATA_QUALITY_AI_PACKAGE.STAGE_CONTENT.APP_STAGE/v1_0_0/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://doc_snippets.py @DATA_QUALITY_AI_PACKAGE.STAGE_CONTENT.APP_STAGE/v1_0_0/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://requirements.txt @DATA_QUALITY_AI_PACKAGE.STAGE_CONTENT.APP_STAGE/v1_0_0/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
"
```

#### Option B: Using Snowsight UI

1. Navigate to Snowsight UI
2. Create Application Package manually
3. Create stage: `DATA_QUALITY_AI_PACKAGE.STAGE_CONTENT.APP_STAGE`
4. Upload files through UI (Data > Databases > DATA_QUALITY_AI_PACKAGE > Stages)
5. Create version from uploaded files
6. Create test application

### Step 4: Test the Deployed App

```sql
USE ROLE ACCOUNTADMIN;

-- Grant access to test database
GRANT USAGE ON DATABASE SNOWFLAKE_SAMPLE_DATA TO APPLICATION DATA_QUALITY_AI_TEST;
GRANT USAGE ON SCHEMA SNOWFLAKE_SAMPLE_DATA.TPCH_SF1 TO APPLICATION DATA_QUALITY_AI_TEST;
GRANT SELECT ON ALL TABLES IN SCHEMA SNOWFLAKE_SAMPLE_DATA.TPCH_SF1 TO APPLICATION DATA_QUALITY_AI_TEST;

-- Open Streamlit app
-- Navigate to: Apps > DATA_QUALITY_AI_TEST > Streamlit
```

**Test thoroughly:**
- [ ] App loads in Native App environment
- [ ] Can access SNOWFLAKE_SAMPLE_DATA
- [ ] AI rule generation works (using Cortex)
- [ ] All tabs function correctly
- [ ] Can save rules to registry table
- [ ] Export YAML works

### Step 5: Publish to Marketplace

1. **Go to Provider Studio**
   - Navigate to: Data Products > Provider Studio
   - Click "Create Listing"

2. **Configure Listing**
   - Select "Native Application"
   - Choose: DATA_QUALITY_AI_PACKAGE
   - Upload screenshots (create these!)
   - Add demo video (recommended)

3. **Set Pricing**
   ```
   Tier 1 - Starter: $500/month
   - Up to 25 tables
   - 5 users
   - 500 AI generations/month

   Tier 2 - Professional: $1,500/month
   - Up to 150 tables
   - 15 users
   - 2,500 AI generations/month

   Tier 3 - Enterprise: Custom
   - Unlimited tables & users
   - Custom pricing
   ```

4. **Submit for Review**
   - Snowflake reviews all marketplace listings
   - Typically takes 2-4 weeks
   - They'll test security, functionality, documentation

5. **Go Live**
   - Once approved, your app appears on marketplace
   - Customers can install directly
   - You get usage reports and billing through Snowflake

## 📊 Monitoring & Usage

After deployment, track:

```sql
-- Check installed applications
SHOW APPLICATIONS LIKE 'DATA_QUALITY_AI%';

-- View usage (as package owner)
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.MARKETPLACE_PURCHASE_EVENTS
WHERE LISTING_NAME = 'DATA_QUALITY_AI';

-- Monitor errors
SELECT * FROM DATA_QUALITY_AI.INFORMATION_SCHEMA.EVENT_TABLE
WHERE TIMESTAMP > DATEADD(hour, -24, CURRENT_TIMESTAMP());
```

## 🔄 Updating the App

When you release updates:

```sql
-- Create new version
ALTER APPLICATION PACKAGE DATA_QUALITY_AI_PACKAGE
  ADD VERSION v1_1_0
  USING '@DATA_QUALITY_AI_PACKAGE.STAGE_CONTENT.APP_STAGE/v1_1_0/';

-- Set as default
ALTER APPLICATION PACKAGE DATA_QUALITY_AI_PACKAGE
  SET DEFAULT RELEASE DIRECTIVE VERSION = v1_1_0 PATCH = 0;

-- Existing customers can upgrade through UI
```

## 🐛 Troubleshooting

### App won't install
**Problem:** Installation fails with privilege errors
**Solution:** Check manifest.yml privileges match what's needed in setup.sql

### Streamlit won't load
**Problem:** "Application not found" error
**Solution:** Verify streamlit_app.py is uploaded and named correctly (not app.py)

### AI generation fails
**Problem:** Cortex errors when generating rules
**Solution:**
- Ensure Cortex is enabled in your region
- Check account has CORTEX.COMPLETE privileges
- Verify customer has granted USAGE on CORTEX

### Can't access customer data
**Problem:** App can't see customer tables
**Solution:** Customer must grant permissions:
```sql
GRANT USAGE ON DATABASE <their_db> TO APPLICATION DATA_QUALITY_AI;
GRANT USAGE ON SCHEMA <their_schema> TO APPLICATION DATA_QUALITY_AI;
GRANT SELECT ON ALL TABLES IN SCHEMA <their_schema> TO APPLICATION DATA_QUALITY_AI;
```

## 📝 Checklist Before Publishing

- [ ] Tested all features work in Native App environment
- [ ] Created professional screenshots (5-8 images)
- [ ] Recorded demo video (3-5 minutes)
- [ ] Updated README with real contact info
- [ ] Tested with multiple Snowflake accounts
- [ ] Verified pricing tiers are correct
- [ ] Security review complete
- [ ] Documentation is comprehensive
- [ ] Terms of service added
- [ ] Privacy policy added

## 🎯 Next Steps

After marketplace listing is live:

1. **Marketing**
   - Blog post announcing launch
   - LinkedIn/Twitter posts
   - Email existing customers
   - Snowflake co-marketing opportunities

2. **Customer Success**
   - Set up support email/Slack
   - Create video tutorials
   - Build knowledge base
   - Plan onboarding process

3. **Iteration**
   - Collect user feedback
   - Monitor usage analytics
   - Plan v1.1.0 features
   - Regular updates (monthly/quarterly)

## 📞 Support

Questions? Contact:
- **Snowflake Partner Support:** partner-support@snowflake.com
- **Native Apps Documentation:** https://docs.snowflake.com/en/developer-guide/native-apps/native-apps-about

---

**Good luck with your launch! 🚀**
