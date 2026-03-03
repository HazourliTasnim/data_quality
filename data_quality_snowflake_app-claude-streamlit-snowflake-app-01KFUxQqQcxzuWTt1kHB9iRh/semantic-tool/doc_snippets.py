"""
Documentation Snippets Module

Provides contextual documentation for source systems and entity types
to improve AI-generated descriptions and data quality rules.
"""

from typing import Dict, List, Optional

# Documentation snippets organized by (source_system, entity_type)
DOCUMENTATION_SNIPPETS: Dict[tuple, List[str]] = {
    ("SAP_SD", "SalesOrderHeader"): [
        "SAP Sales Orders contain header-level information including document number, customer, order date, and total value.",
        "Document numbers follow SAP standard patterns and must be unique within the system.",
        "Order dates cannot be in the future and typically should not be older than 2 years in active reporting tables."
    ],

    ("SAP_SD", "SalesOrderLine"): [
        "SAP Sales Order line items represent individual products/materials ordered.",
        "Each line has an item number (typically 10, 20, 30...) that is unique within the order.",
        "Quantities and prices must be positive values; negative values indicate returns or cancellations."
    ],

    ("SFDC", "Account"): [
        "Salesforce Accounts represent companies or organizations in the CRM.",
        "Account IDs are 18-character Salesforce standard IDs that are globally unique.",
        "Account names are mandatory and typically represent the legal company name."
    ],

    ("SFDC", "Opportunity"): [
        "Salesforce Opportunities track potential sales deals through various stages.",
        "Opportunity amounts represent the expected revenue and should generally be positive.",
        "Stage is a picklist value that must match configured values in Salesforce.",
        "Every opportunity must be associated with an Account via AccountId."
    ],

    ("NETSUITE", "Customer"): [
        "NetSuite Customer records represent both prospects and active customers.",
        "Entity IDs are internal NetSuite identifiers that are system-generated and unique.",
        "Customer email addresses are used for communications and should follow standard email format."
    ],

    ("NETSUITE", "Invoice"): [
        "NetSuite Invoices are financial documents with unique invoice numbers.",
        "Invoice totals include line items, taxes, and discounts.",
        "Invoice dates determine the accounting period and must be valid business dates.",
        "Negative invoice amounts typically indicate credit memos, not regular invoices."
    ],

    ("SAP_C4C", "C4C_Account"): [
        "SAP C4C Accounts represent business partners (customers, prospects) in the cloud CRM.",
        "Account IDs are unique identifiers assigned by the C4C system.",
        "Account status must be one of: Active, Inactive, or Prospect.",
        "Country codes follow ISO standards and are typically 2-character codes."
    ],

    ("SAP_C4C", "C4C_Opportunity"): [
        "C4C Opportunities track sales deals with stages, probabilities, and expected revenue.",
        "Opportunity IDs are system-generated and globally unique within C4C.",
        "Expected revenue must be positive and match the sum of opportunity line items.",
        "Probability values range from 0-100 representing likelihood of closure."
    ],

    ("SAP_APO", "APO_Product"): [
        "APO Product master data contains planning-relevant product information.",
        "Product IDs must be unique and match corresponding ERP product codes.",
        "Safety stock values represent minimum inventory levels and should be non-negative.",
        "Lead times are specified in days and affect planning calculations."
    ],

    ("SAP_APO", "APO_SalesForecast"): [
        "SAP APO Sales Forecasts contain predicted demand by product and location.",
        "Forecast quantities should be non-negative and based on statistical models.",
        "Forecast dates represent the planning period (typically weekly or monthly buckets).",
        "Forecast accuracy is critical for supply chain planning efficiency."
    ],

    ("SAP_APO", "APO_LocationProduct"): [
        "LocationProduct defines product availability and parameters per planning location.",
        "Each combination of location and product must be unique.",
        "Lot sizes define minimum order quantities for procurement or production.",
        "Stock levels are synchronized from ERP systems and must match physical inventory."
    ],

    ("SHOPIFY", "WebOrder"): [
        "Shopify orders represent ecommerce transactions with order numbers, customer info, and line items.",
        "Order IDs are unique across the Shopify store.",
        "Order totals include product costs, shipping, taxes, and discounts.",
        "Order status values include: pending, paid, fulfilled, cancelled, refunded."
    ],

    ("MAGENTO", "ProductCatalog"): [
        "Magento Product Catalog contains product master data for ecommerce.",
        "SKUs are unique product identifiers and must be populated.",
        "Product prices should be positive values in the store's base currency.",
        "Product status (enabled/disabled) controls visibility in the storefront."
    ],

    ("GENERIC", "GENERIC"): [
        "ID columns typically serve as primary keys and should always be populated.",
        "Timestamp columns (created_at, updated_at) are used for audit trails.",
        "Status codes should be constrained to a known set of valid values.",
        "Foreign key relationships must reference valid records in parent tables."
    ]
}

# Business domain descriptions
BUSINESS_DOMAIN_INFO: Dict[str, str] = {
    "Sales": "Sales domain covers customer interactions, orders, opportunities, and revenue tracking.",
    "Finance": "Finance domain includes invoices, payments, general ledger, and financial reporting.",
    "HR": "Human Resources domain manages employee data, payroll, benefits, and organizational structure.",
    "SupplyChain": "Supply Chain covers inventory, procurement, warehousing, and logistics.",
    "Marketing": "Marketing domain tracks campaigns, leads, marketing spend, and attribution.",
    "Customer Service": "Customer Service manages support tickets, cases, and customer interactions."
}

# Source system descriptions
SOURCE_SYSTEM_INFO: Dict[str, str] = {
    "SAP_SD": "SAP Sales & Distribution module handles order-to-cash processes.",
    "SAP_MM": "SAP Materials Management module covers procurement and inventory.",
    "SAP_FI": "SAP Financial Accounting module manages financial transactions.",
    "SAP_CO": "SAP Controlling module handles cost accounting and management reporting.",
    "SAP_APO": "SAP Advanced Planning & Optimizer for supply chain planning and demand forecasting.",
    "SAP_C4C": "SAP Cloud for Customer (C4C) is a cloud-based CRM for sales and service.",
    "SFDC": "Salesforce CRM tracks customer relationships, sales pipeline, and opportunities.",
    "NETSUITE": "NetSuite ERP provides integrated business management including financials and CRM.",
    "DYNAMICS_365": "Microsoft Dynamics 365 ERP/CRM for business applications.",
    "CUSTOM_CRM": "Custom-built CRM system specific to the organization.",
    "CUSTOM_ERP": "Custom-built ERP system specific to the organization.",
    "ORACLE_EBS": "Oracle E-Business Suite for enterprise resource planning.",
    "SHOPIFY": "Shopify ecommerce platform for online stores and retail POS.",
    "MAGENTO": "Magento ecommerce platform for customizable online stores.",
    "WORKDAY": "Workday HCM manages human capital including HR, payroll, and talent.",
    "MARKETO": "Marketo marketing automation platform tracks campaigns and lead nurturing.",
    "ZENDESK": "Zendesk customer service platform manages support tickets and customer inquiries.",
    "GENERIC": "Generic or unknown source system."
}


def get_documentation_snippets(source_system: str, entity_type: str) -> List[str]:
    """
    Get documentation snippets for a specific source system and entity type.

    Args:
        source_system: Source system identifier (e.g., "SAP_SD", "SFDC")
        entity_type: Entity type (e.g., "SalesOrderHeader", "Account")

    Returns:
        List of documentation snippets, or empty list if none found
    """
    # Try exact match first
    key = (source_system, entity_type)
    if key in DOCUMENTATION_SNIPPETS:
        return DOCUMENTATION_SNIPPETS[key]

    # Try generic for this source system
    generic_key = (source_system, "GENERIC")
    if generic_key in DOCUMENTATION_SNIPPETS:
        return DOCUMENTATION_SNIPPETS[generic_key]

    # Fall back to fully generic
    return DOCUMENTATION_SNIPPETS.get(("GENERIC", "GENERIC"), [])


def get_business_domain_info(business_domain: str) -> Optional[str]:
    """Get description for a business domain."""
    return BUSINESS_DOMAIN_INFO.get(business_domain)


def get_source_system_info(source_system: str) -> Optional[str]:
    """Get description for a source system."""
    return SOURCE_SYSTEM_INFO.get(source_system)


def build_context_prompt(
    source_system: str,
    entity_type: Optional[str] = None,
    business_domain: Optional[str] = None
) -> str:
    """
    Build a context prompt section with relevant documentation.

    Args:
        source_system: Source system identifier
        entity_type: Optional entity type
        business_domain: Optional business domain

    Returns:
        Formatted context string for inclusion in LLM prompts
    """
    context_parts = []

    # Add source system info
    sys_info = get_source_system_info(source_system)
    if sys_info:
        context_parts.append(f"Source System Context: {sys_info}")

    # Add business domain info
    if business_domain:
        domain_info = get_business_domain_info(business_domain)
        if domain_info:
            context_parts.append(f"Business Domain: {domain_info}")

    # Add entity-specific documentation
    if entity_type:
        snippets = get_documentation_snippets(source_system, entity_type)
        if snippets:
            context_parts.append("Entity Documentation:")
            for i, snippet in enumerate(snippets, 1):
                context_parts.append(f"  {i}. {snippet}")

    return "\n".join(context_parts) if context_parts else ""


# List of available source systems for dropdowns
AVAILABLE_SOURCE_SYSTEMS = [
    "SAP_SD",
    "SAP_MM",
    "SAP_FI",
    "SAP_CO",
    "SAP_APO",
    "SAP_C4C",
    "SFDC",
    "NETSUITE",
    "DYNAMICS_365",
    "CUSTOM_CRM",
    "CUSTOM_ERP",
    "ORACLE_EBS",
    "SHOPIFY",
    "MAGENTO",
    "WORKDAY",
    "MARKETO",
    "ZENDESK",
    "GENERIC"
]

# List of common business domains
AVAILABLE_BUSINESS_DOMAINS = [
    "Sales",
    "Finance",
    "HR",
    "SupplyChain",
    "Marketing",
    "Customer Service",
    "Operations",
    "Product"
]

# List of common entity types (organized by domain)
COMMON_ENTITY_TYPES = [
    # Sales
    "SalesOrderHeader",
    "SalesOrderLine",
    "Quotation",
    "InvoiceHeader",
    "InvoiceLine",
    "ShipmentHeader",
    "ShipmentLine",
    "WebOrder",
    "WebOrderLine",
    # CRM / Customer
    "Customer",
    "Contact",
    "Account",
    "Lead",
    "Opportunity",
    "C4C_Account",
    "C4C_Contact",
    "C4C_Lead",
    "C4C_Opportunity",
    "C4C_Activity",
    "C4C_ServiceRequest",
    "C4C_Ticket",
    "C4C_VisitPlan",
    # Finance
    "GLHeader",
    "GLLine",
    "Vendor",
    "APInvoice",
    "ARInvoice",
    # Procurement / Supply Chain
    "PurchaseOrderHeader",
    "PurchaseOrderLine",
    "GoodsReceipt",
    "InventoryItem",
    "StockMovement",
    # SAP APO
    "APO_Product",
    "APO_Location",
    "APO_LocationProduct",
    "APO_SalesForecast",
    "APO_PlannedOrder",
    "APO_Requirement",
    "APO_StockProjection",
    "APO_DemandPlan",
    # Ecommerce
    "ProductCatalog",
    "CustomerProfile",
    # Other
    "Payment",
    "Person",
    "Employee",
    "Transaction",
    "Product",
    "GENERIC"
]
