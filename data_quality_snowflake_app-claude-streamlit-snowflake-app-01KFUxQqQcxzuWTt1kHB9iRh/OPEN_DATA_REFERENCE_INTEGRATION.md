# Open Data Reference Integration - Architecture Design

## Executive Summary

This document describes the architecture for integrating external open data references (like INSEE SIRET, ISO codes, postal databases) into the data quality validation framework. The design prioritizes **scalability**, **extensibility**, and **performance**.

### Key Capabilities

- ✅ Validate business identifiers against official registries (INSEE SIRET, DUNS, VAT)
- ✅ Verify addresses against postal databases
- ✅ Check country/currency codes against ISO standards
- ✅ Validate company information against company registries
- ✅ Plugin architecture for adding new reference sources
- ✅ Intelligent caching to minimize API calls
- ✅ Rate limiting and quota management
- ✅ Batch validation for performance

---

## Problem Statement

### Current Limitation

The tool validates data using **internal rules** (NOT_NULL, PATTERN, etc.) but cannot validate against **external authoritative sources**.

**Example Scenarios:**

1. **French Business Validation:**
   - User has `siret` column with 14-digit codes
   - Need to verify: Is this a real, active business in INSEE registry?
   - Current: Can only check format (14 digits)
   - Needed: Check against official INSEE API

2. **Address Validation:**
   - User has postal codes
   - Need to verify: Is this a valid postal code for the given country/city?
   - Current: Can only check pattern
   - Needed: Check against postal authority database

3. **Company Information:**
   - User has company names
   - Need to verify: Does this match official company registry?
   - Current: Can only check for nulls/format
   - Needed: Lookup in official registry

### Requirements

1. **Extensibility:** Easy to add new reference data sources
2. **Performance:** Handle millions of records efficiently
3. **Reliability:** Graceful degradation if external API fails
4. **Cost-Effective:** Minimize API calls (caching, batching)
5. **Freshness:** Keep reference data reasonably up-to-date
6. **Privacy:** Handle sensitive data appropriately

---

## Architecture Overview

### High-Level Design

```
┌─────────────────────────────────────────────────────────┐
│                  Data Quality Tool                       │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  ┌──────────────────────────────────────────────┐       │
│  │         Rule Execution Engine                 │       │
│  │  (execute_column_rule, execute_table_rule)   │       │
│  └─────────────────┬────────────────────────────┘       │
│                    │                                     │
│                    │ calls                               │
│                    ↓                                     │
│  ┌──────────────────────────────────────────────┐       │
│  │   Reference Data Validation Layer            │       │
│  │   - EXTERNAL_REFERENCE rule type             │       │
│  │   - Provider routing                         │       │
│  │   - Batch optimization                       │       │
│  └─────────────────┬────────────────────────────┘       │
│                    │                                     │
│                    │ queries                             │
│                    ↓                                     │
│  ┌──────────────────────────────────────────────┐       │
│  │       Reference Data Manager                 │       │
│  │   - Caching Layer (Snowflake tables)         │       │
│  │   - Cache invalidation strategy              │       │
│  │   - Provider registry                        │       │
│  └─────────────────┬────────────────────────────┘       │
│                    │                                     │
│         ┌──────────┴──────────┬──────────┬──────────┐   │
│         │                     │          │          │   │
│         ↓                     ↓          ↓          ↓   │
│  ┌──────────┐  ┌──────────┐  ┌──────┐  ┌──────────┐   │
│  │  INSEE   │  │  ISO     │  │ USPS │  │  Custom  │   │
│  │ Provider │  │ Provider │  │ Prov.│  │ Provider │   │
│  └────┬─────┘  └────┬─────┘  └───┬──┘  └────┬─────┘   │
│       │             │             │          │          │
└───────┼─────────────┼─────────────┼──────────┼──────────┘
        │             │             │          │
        ↓             ↓             ↓          ↓
┌─────────────────────────────────────────────────────────┐
│              External Data Sources                       │
├─────────────────────────────────────────────────────────┤
│  INSEE API    ISO Standards    USPS API    Custom DB    │
└─────────────────────────────────────────────────────────┘
```

### Component Layers

#### Layer 1: Rule Execution Engine (Existing)
- Executes data quality rules
- Delegates to appropriate rule handler
- New: Calls EXTERNAL_REFERENCE handler

#### Layer 2: Reference Data Validation Layer (New)
- Handles EXTERNAL_REFERENCE rule type
- Routes to appropriate provider
- Optimizes batch lookups
- Handles errors gracefully

#### Layer 3: Reference Data Manager (New)
- Manages cache tables in Snowflake
- Implements cache invalidation policies
- Maintains provider registry
- Handles rate limiting

#### Layer 4: Reference Data Providers (New)
- Plugin interface for external sources
- Provider implementations (INSEE, ISO, etc.)
- API clients with retry logic
- Data transformation/mapping

---

## Reference Data Provider Framework

### Provider Interface

```python
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from datetime import datetime

class ReferenceDataProvider(ABC):
    """
    Abstract base class for all reference data providers.
    Each provider (INSEE, ISO, etc.) implements this interface.
    """

    @property
    @abstractmethod
    def provider_id(self) -> str:
        """Unique identifier for this provider (e.g., 'insee_siret')"""
        pass

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Human-readable name (e.g., 'INSEE SIRET Registry')"""
        pass

    @property
    @abstractmethod
    def supported_fields(self) -> List[str]:
        """List of field types this provider can validate"""
        pass

    @abstractmethod
    def validate_single(self, value: Any, context: Dict = None) -> Dict:
        """
        Validate a single value.

        Args:
            value: The value to validate (e.g., SIRET code)
            context: Optional context (e.g., expected country)

        Returns:
            {
                "is_valid": True/False,
                "exists": True/False,
                "status": "ACTIVE/INACTIVE/NOT_FOUND",
                "details": {...},  # Provider-specific details
                "timestamp": datetime,
                "source": "API/CACHE"
            }
        """
        pass

    @abstractmethod
    def validate_batch(self, values: List[Any], context: Dict = None) -> List[Dict]:
        """
        Validate multiple values in batch (for performance).

        Returns list of validation results in same order as input.
        """
        pass

    @abstractmethod
    def get_cache_ttl(self) -> int:
        """Return cache time-to-live in seconds"""
        pass

    @abstractmethod
    def get_rate_limit(self) -> Dict:
        """
        Return rate limit configuration.

        Returns:
            {
                "calls_per_minute": 60,
                "calls_per_day": 10000,
                "batch_size": 100
            }
        """
        pass

    def get_additional_fields(self, value: Any) -> Dict:
        """
        Optional: Fetch additional enrichment data.

        For INSEE: Get company name, address, status, etc.
        """
        return {}
```

### Provider Registry

```python
class ReferenceDataRegistry:
    """
    Central registry for all reference data providers.
    Allows dynamic registration and provider selection.
    """

    def __init__(self):
        self._providers: Dict[str, ReferenceDataProvider] = {}

    def register(self, provider: ReferenceDataProvider):
        """Register a new provider"""
        self._providers[provider.provider_id] = provider

    def get_provider(self, provider_id: str) -> Optional[ReferenceDataProvider]:
        """Get provider by ID"""
        return self._providers.get(provider_id)

    def list_providers(self) -> List[Dict]:
        """List all registered providers"""
        return [
            {
                "id": p.provider_id,
                "name": p.provider_name,
                "supported_fields": p.supported_fields
            }
            for p in self._providers.values()
        ]

    def find_providers_for_field(self, field_name: str) -> List[ReferenceDataProvider]:
        """Find all providers that can validate a given field type"""
        return [
            p for p in self._providers.values()
            if any(pattern in field_name.lower() for pattern in p.supported_fields)
        ]
```

---

## Implementation: INSEE SIRET Provider

### SIRET Background

- **SIRET:** 14-digit French business establishment identifier
- **Format:** 9-digit SIREN (company) + 5-digit NIC (establishment)
- **API:** INSEE provides Sirene API for lookups
- **Free Tier:** Limited requests per day
- **Use Case:** Validate customer/vendor SIRET codes

### INSEE Provider Implementation

```python
import requests
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import hashlib

class INSEESiretProvider(ReferenceDataProvider):
    """
    Provider for validating French SIRET codes against INSEE Sirene API.

    API Documentation: https://api.insee.fr/catalogue/
    """

    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_base_url = "https://api.insee.fr/entreprises/sirene/V3"
        self._access_token = None
        self._token_expiry = None

    @property
    def provider_id(self) -> str:
        return "insee_siret"

    @property
    def provider_name(self) -> str:
        return "INSEE SIRET Registry (France)"

    @property
    def supported_fields(self) -> List[str]:
        return ["siret", "siren", "french_business_id", "etablissement"]

    def _get_access_token(self) -> str:
        """Get OAuth2 access token for INSEE API"""
        if self._access_token and self._token_expiry > datetime.now():
            return self._access_token

        # Request new token
        auth_url = "https://api.insee.fr/token"
        response = requests.post(
            auth_url,
            auth=(self.api_key, self.api_secret),
            data={"grant_type": "client_credentials"}
        )
        response.raise_for_status()

        token_data = response.json()
        self._access_token = token_data["access_token"]
        self._token_expiry = datetime.now() + timedelta(seconds=token_data["expires_in"] - 60)

        return self._access_token

    def validate_single(self, value: Any, context: Dict = None) -> Dict:
        """Validate a single SIRET code"""
        # Input validation
        siret = str(value).strip()

        if not siret or len(siret) != 14 or not siret.isdigit():
            return {
                "is_valid": False,
                "exists": False,
                "status": "INVALID_FORMAT",
                "details": {"error": "SIRET must be 14 digits"},
                "timestamp": datetime.now(),
                "source": "VALIDATION"
            }

        # Check Luhn algorithm (SIRET checksum)
        if not self._validate_luhn(siret):
            return {
                "is_valid": False,
                "exists": False,
                "status": "INVALID_CHECKSUM",
                "details": {"error": "SIRET checksum validation failed"},
                "timestamp": datetime.now(),
                "source": "VALIDATION"
            }

        # Query INSEE API
        try:
            token = self._get_access_token()
            headers = {"Authorization": f"Bearer {token}"}

            response = requests.get(
                f"{self.api_base_url}/siret/{siret}",
                headers=headers,
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()
                etablissement = data.get("etablissement", {})

                # Check if establishment is active
                is_active = etablissement.get("etatAdministratifEtablissement") == "A"

                return {
                    "is_valid": True,
                    "exists": True,
                    "status": "ACTIVE" if is_active else "INACTIVE",
                    "details": {
                        "siret": siret,
                        "siren": etablissement.get("siren"),
                        "company_name": etablissement.get("uniteLegale", {}).get("denominationUniteLegale"),
                        "address": self._format_address(etablissement),
                        "creation_date": etablissement.get("dateCreationEtablissement"),
                        "activity_code": etablissement.get("activitePrincipaleEtablissement"),
                        "employees": etablissement.get("trancheEffectifsEtablissement")
                    },
                    "timestamp": datetime.now(),
                    "source": "API"
                }

            elif response.status_code == 404:
                return {
                    "is_valid": False,
                    "exists": False,
                    "status": "NOT_FOUND",
                    "details": {"error": "SIRET not found in INSEE registry"},
                    "timestamp": datetime.now(),
                    "source": "API"
                }

            else:
                raise Exception(f"API error: {response.status_code}")

        except Exception as e:
            # Graceful degradation on API failure
            return {
                "is_valid": None,  # Unknown
                "exists": None,
                "status": "API_ERROR",
                "details": {"error": str(e)},
                "timestamp": datetime.now(),
                "source": "ERROR"
            }

    def validate_batch(self, values: List[Any], context: Dict = None) -> List[Dict]:
        """
        Validate multiple SIRET codes.

        Note: INSEE API doesn't support batch queries, so we need to:
        1. Check cache first
        2. Make individual API calls with rate limiting
        3. Return results in order
        """
        results = []

        for value in values:
            # Add delay for rate limiting (if needed)
            result = self.validate_single(value, context)
            results.append(result)

        return results

    def get_cache_ttl(self) -> int:
        """Cache for 30 days (company info changes infrequently)"""
        return 30 * 24 * 60 * 60  # 30 days in seconds

    def get_rate_limit(self) -> Dict:
        return {
            "calls_per_minute": 30,  # INSEE free tier limit
            "calls_per_day": 1000,
            "batch_size": 1  # No batch support
        }

    def get_additional_fields(self, value: Any) -> Dict:
        """Get enriched company information"""
        result = self.validate_single(value)
        return result.get("details", {})

    # Helper methods

    def _validate_luhn(self, siret: str) -> bool:
        """Validate SIRET checksum using Luhn algorithm"""
        def luhn_checksum(code):
            def digits_of(n):
                return [int(d) for d in str(n)]
            digits = digits_of(code)
            odd_digits = digits[-1::-2]
            even_digits = digits[-2::-2]
            checksum = sum(odd_digits)
            for d in even_digits:
                checksum += sum(digits_of(d * 2))
            return checksum % 10

        return luhn_checksum(siret) == 0

    def _format_address(self, etablissement: Dict) -> str:
        """Format establishment address from API response"""
        addr = etablissement.get("adresseEtablissement", {})
        parts = [
            addr.get("numeroVoieEtablissement"),
            addr.get("typeVoieEtablissement"),
            addr.get("libelleVoieEtablissement"),
            addr.get("codePostalEtablissement"),
            addr.get("libelleCommuneEtablissement")
        ]
        return " ".join(filter(None, parts))
```

---

## Caching Strategy

### Cache Architecture

```
┌─────────────────────────────────────────────────────┐
│         Snowflake Database: REFERENCE_DATA          │
├─────────────────────────────────────────────────────┤
│                                                      │
│  Table: REFERENCE_CACHE                             │
│  ┌────────────────────────────────────────────┐    │
│  │ PROVIDER_ID      VARCHAR   (e.g., 'insee')│    │
│  │ LOOKUP_KEY       VARCHAR   (e.g., SIRET)  │    │
│  │ LOOKUP_VALUE     VARCHAR   (the value)     │    │
│  │ IS_VALID         BOOLEAN                   │    │
│  │ EXISTS           BOOLEAN                   │    │
│  │ STATUS           VARCHAR                   │    │
│  │ DETAILS          VARIANT   (JSON)          │    │
│  │ CACHED_AT        TIMESTAMP                 │    │
│  │ EXPIRES_AT       TIMESTAMP                 │    │
│  │ HIT_COUNT        INTEGER                   │    │
│  │ PRIMARY KEY      (PROVIDER_ID, LOOKUP_KEY, │    │
│  │                   LOOKUP_VALUE)             │    │
│  └────────────────────────────────────────────┘    │
│                                                      │
│  Table: REFERENCE_ENRICHMENT (optional)             │
│  ┌────────────────────────────────────────────┐    │
│  │ PROVIDER_ID      VARCHAR                   │    │
│  │ LOOKUP_VALUE     VARCHAR                   │    │
│  │ ENRICHED_DATA    VARIANT   (JSON)          │    │
│  │ CACHED_AT        TIMESTAMP                 │    │
│  └────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
```

### Cache Lookup Flow

```python
def validate_with_cache(
    conn: SnowflakeConnection,
    provider: ReferenceDataProvider,
    value: Any
) -> Dict:
    """
    Validate value with cache-first strategy.

    Flow:
    1. Check Snowflake cache
    2. If hit and not expired → return cached result
    3. If miss or expired → call provider API
    4. Store result in cache
    5. Return result
    """
    cursor = conn.cursor()
    provider_id = provider.provider_id

    # 1. Check cache
    cursor.execute("""
        SELECT
            IS_VALID,
            EXISTS,
            STATUS,
            DETAILS,
            CACHED_AT,
            EXPIRES_AT
        FROM REFERENCE_DATA.REFERENCE_CACHE
        WHERE PROVIDER_ID = %s
          AND LOOKUP_VALUE = %s
          AND EXPIRES_AT > CURRENT_TIMESTAMP()
    """, (provider_id, str(value)))

    cached = cursor.fetchone()

    if cached:
        # Cache hit
        cursor.execute("""
            UPDATE REFERENCE_DATA.REFERENCE_CACHE
            SET HIT_COUNT = HIT_COUNT + 1
            WHERE PROVIDER_ID = %s AND LOOKUP_VALUE = %s
        """, (provider_id, str(value)))

        return {
            "is_valid": cached[0],
            "exists": cached[1],
            "status": cached[2],
            "details": json.loads(cached[3]) if cached[3] else {},
            "timestamp": cached[4],
            "source": "CACHE"
        }

    # 2. Cache miss - call provider
    result = provider.validate_single(value)

    # 3. Store in cache
    ttl = provider.get_cache_ttl()
    cursor.execute("""
        INSERT INTO REFERENCE_DATA.REFERENCE_CACHE
        (PROVIDER_ID, LOOKUP_VALUE, IS_VALID, EXISTS, STATUS,
         DETAILS, CACHED_AT, EXPIRES_AT, HIT_COUNT)
        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP(),
                DATEADD(second, %s, CURRENT_TIMESTAMP()), 0)
    """, (
        provider_id,
        str(value),
        result["is_valid"],
        result["exists"],
        result["status"],
        json.dumps(result.get("details", {})),
        ttl
    ))

    return result
```

### Cache Invalidation

**Strategies:**

1. **Time-Based (TTL):**
   - Each provider defines cache TTL
   - INSEE SIRET: 30 days (companies change infrequently)
   - ISO codes: 365 days (very stable)
   - Postal codes: 90 days (occasional changes)

2. **Manual Invalidation:**
   ```sql
   DELETE FROM REFERENCE_DATA.REFERENCE_CACHE
   WHERE PROVIDER_ID = 'insee_siret'
     AND LOOKUP_VALUE = '12345678901234';
   ```

3. **Bulk Refresh:**
   ```sql
   -- Refresh all expired entries for a provider
   DELETE FROM REFERENCE_DATA.REFERENCE_CACHE
   WHERE PROVIDER_ID = 'insee_siret'
     AND EXPIRES_AT < CURRENT_TIMESTAMP();
   ```

4. **Hit Count Based:**
   - Frequently accessed values get refreshed proactively
   - Low hit count entries get purged after expiration

---

## New Rule Type: EXTERNAL_REFERENCE

### Rule Configuration

```yaml
columns:
  - name: siret
    data_type: VARCHAR
    description: "French business establishment identifier"
    logical_type: "business_identifier"
    dq_rules:
      - id: siret_insee_validation
        type: EXTERNAL_REFERENCE
        severity: CRITICAL
        params:
          provider_id: "insee_siret"
          check_exists: true
          check_active: true
          enrich: true  # Optionally fetch additional data
          cache_enabled: true
        description: "Validate SIRET against official INSEE registry"
        lambda_hint: "INSEE_SIRET_LOOKUP(siret)"
```

### Rule Execution

```python
def execute_external_reference_rule(
    conn: SnowflakeConnection,
    database: str,
    schema: str,
    table: str,
    column_name: str,
    rule: dict,
    limit: int = 100
) -> dict:
    """
    Execute EXTERNAL_REFERENCE rule type.

    Process:
    1. Get distinct values from column (deduplicate)
    2. Check cache for each value
    3. Batch API call for cache misses
    4. Update cache
    5. Identify violations
    6. Return results with enrichment
    """
    params = rule.get("params", {})
    provider_id = params.get("provider_id")
    check_exists = params.get("check_exists", True)
    check_active = params.get("check_active", False)
    enrich = params.get("enrich", False)

    # Get provider
    registry = get_reference_registry()
    provider = registry.get_provider(provider_id)

    if not provider:
        raise ValueError(f"Provider '{provider_id}' not found")

    cursor = conn.cursor()
    full_table = f"{database}.{schema}.{table}"

    # 1. Get distinct values to validate (optimization)
    cursor.execute(f"""
        SELECT DISTINCT {column_name} as value, COUNT(*) as row_count
        FROM {full_table}
        WHERE {column_name} IS NOT NULL
        GROUP BY {column_name}
    """)

    distinct_values = [(row[0], row[1]) for row in cursor.fetchall()]

    # 2. Validate each distinct value
    validation_results = {}

    for value, row_count in distinct_values:
        result = validate_with_cache(conn, provider, value)
        validation_results[value] = result

    # 3. Find violations
    violations = []

    for value, result in validation_results.items():
        is_violation = False
        violation_reason = []

        if check_exists and not result.get("exists"):
            is_violation = True
            violation_reason.append("Value not found in reference data")

        if check_active and result.get("status") != "ACTIVE":
            is_violation = True
            violation_reason.append(f"Status is {result.get('status')}, expected ACTIVE")

        if is_violation:
            violations.append({
                "value": value,
                "reason": "; ".join(violation_reason),
                "status": result.get("status"),
                "details": result.get("details", {}) if enrich else None
            })

    # 4. Get sample violation rows
    if violations:
        violation_values = [v["value"] for v in violations[:limit]]
        placeholders = ",".join(["%s"] * len(violation_values))

        cursor.execute(f"""
            SELECT *
            FROM {full_table}
            WHERE {column_name} IN ({placeholders})
            LIMIT {limit}
        """, violation_values)

        violation_rows = [dict(row) for row in cursor.fetchall()]
    else:
        violation_rows = []

    # 5. Calculate metrics
    total_distinct = len(distinct_values)
    violations_distinct = len(violations)

    return {
        "rule_id": rule.get("id"),
        "rule_type": "EXTERNAL_REFERENCE",
        "provider": provider.provider_name,
        "severity": rule.get("severity"),
        "column": column_name,
        "total_distinct_values": total_distinct,
        "distinct_violations": violations_distinct,
        "violation_rate": (violations_distinct / total_distinct * 100) if total_distinct > 0 else 0,
        "violations": violation_rows,
        "violation_summary": violations,
        "enrichment_available": enrich
    }
```

---

## Additional Providers (Examples)

### ISO Country Codes

```python
class ISOCountryProvider(ReferenceDataProvider):
    """Validate country codes against ISO 3166"""

    @property
    def provider_id(self) -> str:
        return "iso_country"

    @property
    def provider_name(self) -> str:
        return "ISO 3166 Country Codes"

    @property
    def supported_fields(self) -> List[str]:
        return ["country", "country_code", "country_iso", "pays"]

    def validate_single(self, value: Any, context: Dict = None) -> Dict:
        # ISO codes rarely change, can be stored locally
        iso_codes = self._load_iso_codes()

        code = str(value).upper().strip()

        if code in iso_codes:
            return {
                "is_valid": True,
                "exists": True,
                "status": "VALID",
                "details": {
                    "code": code,
                    "country_name": iso_codes[code]["name"],
                    "alpha3": iso_codes[code]["alpha3"],
                    "numeric": iso_codes[code]["numeric"]
                },
                "timestamp": datetime.now(),
                "source": "LOCAL"
            }
        else:
            return {
                "is_valid": False,
                "exists": False,
                "status": "NOT_FOUND",
                "details": {"error": f"'{code}' is not a valid ISO country code"},
                "timestamp": datetime.now(),
                "source": "LOCAL"
            }

    def get_cache_ttl(self) -> int:
        return 365 * 24 * 60 * 60  # 1 year
```

### US Postal Codes (USPS)

```python
class USPSPostalCodeProvider(ReferenceDataProvider):
    """Validate US ZIP codes against USPS database"""

    @property
    def provider_id(self) -> str:
        return "usps_zip"

    @property
    def provider_name(self) -> str:
        return "USPS ZIP Code Database"

    @property
    def supported_fields(self) -> List[str]:
        return ["zip", "zip_code", "postal_code", "zipcode"]

    def validate_single(self, value: Any, context: Dict = None) -> Dict:
        # Could use USPS API or local database
        # Implementation similar to INSEE
        pass
```

### VAT Number Validation (EU)

```python
class EUVATProvider(ReferenceDataProvider):
    """Validate EU VAT numbers via VIES"""

    @property
    def provider_id(self) -> str:
        return "eu_vat"

    @property
    def provider_name(self) -> str:
        return "EU VIES VAT Number Validation"

    # ... implementation
```

---

## Scalability Considerations

### 1. Performance Optimization

#### Batch Processing
```python
def execute_reference_validation_batch(
    conn: SnowflakeConnection,
    values: List[Any],
    provider: ReferenceDataProvider
) -> List[Dict]:
    """
    Optimized batch validation:
    1. Deduplicate input values
    2. Check cache for all values in single query
    3. Group cache misses into batches
    4. Call provider batch API
    5. Update cache in bulk
    """
    # Deduplicate
    unique_values = list(set(values))

    # Bulk cache lookup
    cursor = conn.cursor()
    placeholders = ",".join(["%s"] * len(unique_values))

    cursor.execute(f"""
        SELECT LOOKUP_VALUE, IS_VALID, EXISTS, STATUS, DETAILS
        FROM REFERENCE_DATA.REFERENCE_CACHE
        WHERE PROVIDER_ID = %s
          AND LOOKUP_VALUE IN ({placeholders})
          AND EXPIRES_AT > CURRENT_TIMESTAMP()
    """, [provider.provider_id] + unique_values)

    cached_results = {row[0]: row for row in cursor.fetchall()}

    # Identify cache misses
    cache_misses = [v for v in unique_values if v not in cached_results]

    # Batch API call for misses
    if cache_misses:
        api_results = provider.validate_batch(cache_misses)

        # Bulk insert to cache
        insert_data = [
            (
                provider.provider_id,
                value,
                result["is_valid"],
                result["exists"],
                result["status"],
                json.dumps(result.get("details", {})),
                provider.get_cache_ttl()
            )
            for value, result in zip(cache_misses, api_results)
        ]

        cursor.executemany("""
            INSERT INTO REFERENCE_DATA.REFERENCE_CACHE
            (PROVIDER_ID, LOOKUP_VALUE, IS_VALID, EXISTS, STATUS,
             DETAILS, CACHED_AT, EXPIRES_AT, HIT_COUNT)
            VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP(),
                    DATEADD(second, %s, CURRENT_TIMESTAMP()), 0)
        """, insert_data)

    # Combine results
    all_results = {}
    all_results.update(cached_results)
    all_results.update({v: api_results[i] for i, v in enumerate(cache_misses)})

    return [all_results[v] for v in values]  # Maintain original order
```

#### Parallel Processing

For very large tables:

```python
def execute_reference_validation_parallel(
    conn: SnowflakeConnection,
    full_table: str,
    column_name: str,
    provider: ReferenceDataProvider,
    num_partitions: int = 10
) -> Dict:
    """
    Partition table and process in parallel.

    Uses Snowflake's HASH() function to partition data.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def process_partition(partition_id: int):
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT DISTINCT {column_name}
            FROM {full_table}
            WHERE MOD(HASH({column_name}), {num_partitions}) = {partition_id}
              AND {column_name} IS NOT NULL
        """)

        values = [row[0] for row in cursor.fetchall()]
        return execute_reference_validation_batch(conn, values, provider)

    # Process partitions in parallel
    with ThreadPoolExecutor(max_workers=num_partitions) as executor:
        futures = [
            executor.submit(process_partition, i)
            for i in range(num_partitions)
        ]

        all_results = []
        for future in as_completed(futures):
            all_results.extend(future.result())

    return aggregate_results(all_results)
```

### 2. Cost Management

#### API Cost Tracking

```sql
CREATE TABLE REFERENCE_DATA.API_USAGE (
    PROVIDER_ID VARCHAR,
    CALL_TYPE VARCHAR,  -- 'single', 'batch'
    CALLS_MADE INTEGER,
    VALUES_VALIDATED INTEGER,
    COST_ESTIMATE DECIMAL(10, 4),
    TIMESTAMP TIMESTAMP,
    USER_ID VARCHAR
);

-- Track usage
INSERT INTO REFERENCE_DATA.API_USAGE
VALUES ('insee_siret', 'single', 1, 1, 0.001, CURRENT_TIMESTAMP(), CURRENT_USER());
```

#### Cost Optimization Strategies

1. **Aggressive Caching:** 30-90 day TTLs for stable data
2. **Deduplication:** Validate distinct values only
3. **Batch API Calls:** Where supported by provider
4. **Lazy Validation:** Validate on-demand vs full table scans
5. **Sampling:** Validate random sample for quality estimation

### 3. Error Handling

```python
class ReferenceValidationError(Exception):
    """Base exception for reference validation errors"""
    pass

class ProviderAPIError(ReferenceValidationError):
    """External API unavailable or returned error"""
    pass

class RateLimitExceeded(ReferenceValidationError):
    """API rate limit exceeded"""
    pass

class CacheError(ReferenceValidationError):
    """Cache read/write failed"""
    pass

# Graceful degradation
def validate_with_fallback(
    conn: SnowflakeConnection,
    provider: ReferenceDataProvider,
    value: Any,
    fallback_to_format: bool = True
) -> Dict:
    """
    Validate with graceful degradation.

    If API fails:
    1. Return cached result if available (even if expired)
    2. Fall back to format validation only
    3. Log error for investigation
    """
    try:
        return validate_with_cache(conn, provider, value)

    except ProviderAPIError as e:
        # Check for stale cache
        result = get_stale_cache(conn, provider.provider_id, value)
        if result:
            result["source"] = "STALE_CACHE"
            result["warning"] = "Using expired cache due to API error"
            return result

        # Fall back to format validation
        if fallback_to_format:
            if provider.provider_id == "insee_siret":
                is_valid_format = len(str(value)) == 14 and str(value).isdigit()
                return {
                    "is_valid": is_valid_format,
                    "exists": None,
                    "status": "FORMAT_ONLY",
                    "details": {"warning": "API unavailable, format check only"},
                    "timestamp": datetime.now(),
                    "source": "FALLBACK"
                }

        raise
```

---

## UI Integration

### Configuration Screen

```
┌─────────────────────────────────────────────────────────┐
│  ⚙️ Reference Data Providers                            │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  Active Providers:                                       │
│  ┌──────────────────────────────────────────────────┐  │
│  │ ✅ INSEE SIRET (France)                          │  │
│  │    API Status: Connected                         │  │
│  │    Cache Hit Rate: 87%                           │  │
│  │    API Calls Today: 247 / 1000                   │  │
│  │    [ Configure ] [ Test Connection ]             │  │
│  ├──────────────────────────────────────────────────┤  │
│  │ ✅ ISO Country Codes                             │  │
│  │    Source: Local Database                        │  │
│  │    Last Updated: 2025-01-15                      │  │
│  │    [ Configure ]                                 │  │
│  ├──────────────────────────────────────────────────┤  │
│  │ ⚪ EU VAT Validation                             │  │
│  │    Status: Not Configured                        │  │
│  │    [ Configure ] [ Enable ]                      │  │
│  └──────────────────────────────────────────────────┘  │
│                                                          │
│  [ + Add Custom Provider ]                              │
└─────────────────────────────────────────────────────────┘
```

### Rule Creation UI

When user adds EXTERNAL_REFERENCE rule:

```
┌─────────────────────────────────────────────────────────┐
│  Add External Reference Rule                            │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  Column: [siret ▼]                                      │
│                                                          │
│  Provider: [INSEE SIRET Registry ▼]                    │
│                                                          │
│  Validation Options:                                     │
│  ☑ Check if value exists in registry                   │
│  ☑ Check if status is ACTIVE                           │
│  ☑ Enable caching (recommended)                        │
│  ☐ Fetch enrichment data (company name, address)       │
│                                                          │
│  Severity: ⚫ CRITICAL  ○ WARNING  ○ INFO              │
│                                                          │
│  Estimated Cost: ~€0.50 per 1000 validations            │
│                                                          │
│  [ Cancel ]  [ Add Rule ]                               │
└─────────────────────────────────────────────────────────┘
```

### Validation Results

```
┌─────────────────────────────────────────────────────────┐
│  Validation Results: SIRET Column                        │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  Total Records: 10,000                                   │
│  Distinct Values: 8,547                                  │
│  Violations: 127 (1.3%)                                  │
│                                                          │
│  Breakdown:                                              │
│  ├─ Invalid Format: 23                                  │
│  ├─ Not Found in Registry: 89                           │
│  └─ Inactive Status: 15                                 │
│                                                          │
│  Cache Performance:                                      │
│  ├─ Cache Hits: 7,234 (84.6%)                          │
│  ├─ API Calls Made: 1,313                              │
│  └─ Estimated Cost: €1.31                               │
│                                                          │
│  [ Download Violations CSV ]  [ View Details ]          │
└─────────────────────────────────────────────────────────┘
```

---

## Deployment Checklist

### Phase 1: Core Framework
- [ ] Implement ReferenceDataProvider interface
- [ ] Create ReferenceDataRegistry
- [ ] Set up cache tables in Snowflake
- [ ] Implement cache lookup/storage functions
- [ ] Add EXTERNAL_REFERENCE rule type

### Phase 2: INSEE SIRET Provider
- [ ] Register for INSEE API access
- [ ] Implement INSEESiretProvider
- [ ] Test with sample SIRET codes
- [ ] Configure cache TTL
- [ ] Set up rate limiting

### Phase 3: Additional Providers
- [ ] ISO country codes (local)
- [ ] ISO currency codes (local)
- [ ] Postal code validation (TBD which countries)
- [ ] Custom provider template

### Phase 4: UI Integration
- [ ] Provider configuration screen
- [ ] Rule creation wizard
- [ ] Results visualization
- [ ] Cost tracking dashboard

### Phase 5: Documentation
- [ ] Provider setup guide
- [ ] API key configuration
- [ ] Adding custom providers
- [ ] Troubleshooting

---

## Future Enhancements

### Advanced Features

1. **Data Enrichment:**
   - Auto-populate company name from SIRET
   - Fill address fields from postal code
   - Add country from phone number prefix

2. **Smart Suggestions:**
   - AI recommends providers based on column name
   - Auto-detect reference data needs
   - Suggest corrections for invalid values

3. **Multi-Provider Validation:**
   - Validate against multiple sources
   - Cross-check consistency
   - Confidence scoring

4. **Historical Tracking:**
   - Track when values become invalid
   - Alert on status changes
   - Audit trail for reference data

5. **Provider Marketplace:**
   - Community-contributed providers
   - Provider ratings and reviews
   - One-click provider installation

---

## Conclusion

This Open Data Reference Integration architecture provides:

✅ **Extensible:** Easy to add new providers
✅ **Performant:** Intelligent caching, batch processing
✅ **Cost-Effective:** Minimizes API calls
✅ **Reliable:** Graceful degradation on failures
✅ **User-Friendly:** Clear UI for configuration
✅ **Enterprise-Ready:** Audit trails, cost tracking

**Next Steps:**
1. Implement core framework
2. Deploy INSEE SIRET provider (pilot)
3. Gather user feedback
4. Add additional providers based on demand
5. Scale to production workloads
