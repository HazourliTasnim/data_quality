"""
Reference Data Providers Framework

This module provides the infrastructure for integrating external reference data sources
(like INSEE SIRET, ISO codes, postal databases) into data quality validation.

Architecture:
- Abstract Provider Interface: All providers implement ReferenceDataProvider
- Provider Registry: Central registry for provider discovery and routing
- Caching Layer: Snowflake-based caching to minimize API calls
- Batch Optimization: Efficient bulk validation

Example Providers:
- INSEE SIRET: French business identifier validation
- ISO Country/Currency: Standard code validation
- USPS: US postal code validation
- Custom providers as needed
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import json
import requests
from snowflake.connector import SnowflakeConnection


# ============================================================================
# Abstract Provider Interface
# ============================================================================

class ReferenceDataProvider(ABC):
    """
    Abstract base class for all reference data providers.

    Each provider (INSEE, ISO, etc.) implements this interface to provide:
    - Validation of values against external reference data
    - Caching configuration
    - Rate limiting configuration
    - Optional data enrichment
    """

    @property
    @abstractmethod
    def provider_id(self) -> str:
        """
        Unique identifier for this provider (e.g., 'insee_siret').
        Used for cache keys and configuration.
        """
        pass

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Human-readable name (e.g., 'INSEE SIRET Registry')"""
        pass

    @property
    @abstractmethod
    def supported_fields(self) -> List[str]:
        """
        List of field name patterns this provider can validate.

        Example: ['siret', 'siren', 'french_business_id']
        Used for auto-detection of appropriate provider.
        """
        pass

    @abstractmethod
    def validate_single(self, value: Any, context: Dict = None) -> Dict:
        """
        Validate a single value.

        Args:
            value: The value to validate (e.g., SIRET code)
            context: Optional context (e.g., expected country, date)

        Returns:
            {
                "is_valid": True/False,  # Overall validity
                "exists": True/False,    # Exists in reference data
                "status": "ACTIVE/INACTIVE/NOT_FOUND/INVALID_FORMAT",
                "details": {...},        # Provider-specific details
                "timestamp": datetime,
                "source": "API/CACHE/VALIDATION/LOCAL"
            }
        """
        pass

    @abstractmethod
    def validate_batch(self, values: List[Any], context: Dict = None) -> List[Dict]:
        """
        Validate multiple values in batch (for performance).

        Returns list of validation results in same order as input.
        Default implementation calls validate_single() for each value.
        """
        pass

    @abstractmethod
    def get_cache_ttl(self) -> int:
        """
        Return cache time-to-live in seconds.

        Examples:
        - INSEE SIRET: 30 days (2,592,000 seconds)
        - ISO codes: 365 days (31,536,000 seconds)
        - Dynamic data: 1 hour (3,600 seconds)
        """
        pass

    @abstractmethod
    def get_rate_limit(self) -> Dict:
        """
        Return rate limit configuration.

        Returns:
            {
                "calls_per_minute": 60,
                "calls_per_day": 10000,
                "batch_size": 100  # Max values per batch call
            }
        """
        pass

    def get_additional_fields(self, value: Any) -> Dict:
        """
        Optional: Fetch additional enrichment data.

        For INSEE: Get company name, address, status, etc.
        For postal: Get city, state, coordinates, etc.

        Returns dict of field_name -> value
        """
        return {}


# ============================================================================
# Provider Registry
# ============================================================================

class ReferenceDataRegistry:
    """
    Central registry for all reference data providers.

    Manages provider lifecycle:
    - Registration
    - Discovery (find provider for a field)
    - Provider lookup
    """

    def __init__(self):
        self._providers: Dict[str, ReferenceDataProvider] = {}

    def register(self, provider: ReferenceDataProvider):
        """Register a new provider"""
        self._providers[provider.provider_id] = provider
        print(f"✓ Registered provider: {provider.provider_name} ({provider.provider_id})")

    def get_provider(self, provider_id: str) -> Optional[ReferenceDataProvider]:
        """Get provider by ID"""
        return self._providers.get(provider_id)

    def list_providers(self) -> List[Dict]:
        """List all registered providers with metadata"""
        return [
            {
                "id": p.provider_id,
                "name": p.provider_name,
                "supported_fields": p.supported_fields,
                "cache_ttl_days": p.get_cache_ttl() / (24 * 60 * 60),
                "rate_limit": p.get_rate_limit()
            }
            for p in self._providers.values()
        ]

    def find_providers_for_field(self, field_name: str) -> List[ReferenceDataProvider]:
        """
        Find all providers that can validate a given field based on name.

        Matches if any supported pattern is contained in field_name (case-insensitive).
        """
        field_lower = field_name.lower()
        matches = []

        for provider in self._providers.values():
            for pattern in provider.supported_fields:
                if pattern.lower() in field_lower:
                    matches.append(provider)
                    break

        return matches


# Global registry instance
_global_registry = None

def get_reference_registry() -> ReferenceDataRegistry:
    """Get or create the global provider registry"""
    global _global_registry
    if _global_registry is None:
        _global_registry = ReferenceDataRegistry()
        # Auto-register built-in providers
        _register_builtin_providers(_global_registry)
    return _global_registry


def _register_builtin_providers(registry: ReferenceDataRegistry):
    """Register all built-in providers"""
    # ISO Country provider (local data, no API needed)
    try:
        registry.register(ISOCountryProvider())
    except Exception as e:
        print(f"Warning: Could not register ISO Country provider: {e}")

    # Add more built-in providers here as they're implemented


# ============================================================================
# Caching Functions
# ============================================================================

def ensure_cache_tables(conn: SnowflakeConnection):
    """
    Create reference data cache tables if they don't exist.

    Call this during app initialization.
    """
    cursor = conn.cursor()
    try:
        # Create schema
        cursor.execute("CREATE SCHEMA IF NOT EXISTS REFERENCE_DATA")

        # Create cache table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS REFERENCE_DATA.REFERENCE_CACHE (
                PROVIDER_ID VARCHAR(100) NOT NULL,
                LOOKUP_VALUE VARCHAR(500) NOT NULL,
                IS_VALID BOOLEAN,
                EXISTS BOOLEAN,
                STATUS VARCHAR(50),
                DETAILS VARIANT,
                CACHED_AT TIMESTAMP_NTZ NOT NULL,
                EXPIRES_AT TIMESTAMP_NTZ NOT NULL,
                HIT_COUNT INTEGER DEFAULT 0,
                PRIMARY KEY (PROVIDER_ID, LOOKUP_VALUE)
            )
        """)

        # Create usage tracking table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS REFERENCE_DATA.API_USAGE (
                PROVIDER_ID VARCHAR(100),
                CALL_TYPE VARCHAR(20),
                CALLS_MADE INTEGER,
                VALUES_VALIDATED INTEGER,
                COST_ESTIMATE DECIMAL(10, 4),
                TIMESTAMP TIMESTAMP_NTZ,
                USER_ID VARCHAR(100)
            )
        """)

        print("✓ Reference data cache tables initialized")

    finally:
        cursor.close()


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
    lookup_value = str(value)

    try:
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
        """, (provider_id, lookup_value))

        cached = cursor.fetchone()

        if cached:
            # Cache hit - update hit count
            cursor.execute("""
                UPDATE REFERENCE_DATA.REFERENCE_CACHE
                SET HIT_COUNT = HIT_COUNT + 1
                WHERE PROVIDER_ID = %s AND LOOKUP_VALUE = %s
            """, (provider_id, lookup_value))

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
            MERGE INTO REFERENCE_DATA.REFERENCE_CACHE AS target
            USING (SELECT %s AS provider_id, %s AS lookup_value) AS source
            ON target.PROVIDER_ID = source.provider_id
               AND target.LOOKUP_VALUE = source.lookup_value
            WHEN MATCHED THEN UPDATE SET
                IS_VALID = %s,
                EXISTS = %s,
                STATUS = %s,
                DETAILS = PARSE_JSON(%s),
                CACHED_AT = CURRENT_TIMESTAMP(),
                EXPIRES_AT = DATEADD(second, %s, CURRENT_TIMESTAMP())
            WHEN NOT MATCHED THEN INSERT
                (PROVIDER_ID, LOOKUP_VALUE, IS_VALID, EXISTS, STATUS,
                 DETAILS, CACHED_AT, EXPIRES_AT, HIT_COUNT)
            VALUES (%s, %s, %s, %s, %s, PARSE_JSON(%s),
                    CURRENT_TIMESTAMP(), DATEADD(second, %s, CURRENT_TIMESTAMP()), 0)
        """, (
            provider_id, lookup_value,
            result["is_valid"], result["exists"], result["status"],
            json.dumps(result.get("details", {})), ttl,
            provider_id, lookup_value,
            result["is_valid"], result["exists"], result["status"],
            json.dumps(result.get("details", {})), ttl
        ))

        # Track API usage
        cursor.execute("""
            INSERT INTO REFERENCE_DATA.API_USAGE
            (PROVIDER_ID, CALL_TYPE, CALLS_MADE, VALUES_VALIDATED,
             COST_ESTIMATE, TIMESTAMP, USER_ID)
            VALUES (%s, 'single', 1, 1, 0.001, CURRENT_TIMESTAMP(), CURRENT_USER())
        """, (provider_id,))

        return result

    finally:
        cursor.close()


# ============================================================================
# Built-in Providers
# ============================================================================

class ISOCountryProvider(ReferenceDataProvider):
    """
    Validate country codes against ISO 3166 standard.

    Uses local data (no API calls needed).
    """

    # ISO 3166-1 alpha-2 codes (subset for example - expand as needed)
    ISO_COUNTRIES = {
        "FR": {"name": "France", "alpha3": "FRA", "numeric": "250"},
        "US": {"name": "United States", "alpha3": "USA", "numeric": "840"},
        "GB": {"name": "United Kingdom", "alpha3": "GBR", "numeric": "826"},
        "DE": {"name": "Germany", "alpha3": "DEU", "numeric": "276"},
        "ES": {"name": "Spain", "alpha3": "ESP", "numeric": "724"},
        "IT": {"name": "Italy", "alpha3": "ITA", "numeric": "380"},
        "CN": {"name": "China", "alpha3": "CHN", "numeric": "156"},
        "JP": {"name": "Japan", "alpha3": "JPN", "numeric": "392"},
        "CA": {"name": "Canada", "alpha3": "CAN", "numeric": "124"},
        "AU": {"name": "Australia", "alpha3": "AUS", "numeric": "036"},
        # Add more as needed or load from file
    }

    @property
    def provider_id(self) -> str:
        return "iso_country"

    @property
    def provider_name(self) -> str:
        return "ISO 3166 Country Codes"

    @property
    def supported_fields(self) -> List[str]:
        return ["country", "country_code", "country_iso", "pays", "pais"]

    def validate_single(self, value: Any, context: Dict = None) -> Dict:
        code = str(value).upper().strip()

        if code in self.ISO_COUNTRIES:
            return {
                "is_valid": True,
                "exists": True,
                "status": "VALID",
                "details": {
                    "code": code,
                    "country_name": self.ISO_COUNTRIES[code]["name"],
                    "alpha3": self.ISO_COUNTRIES[code]["alpha3"],
                    "numeric": self.ISO_COUNTRIES[code]["numeric"]
                },
                "timestamp": datetime.now(),
                "source": "LOCAL"
            }
        else:
            return {
                "is_valid": False,
                "exists": False,
                "status": "NOT_FOUND",
                "details": {"error": f"'{code}' is not a valid ISO 3166 country code"},
                "timestamp": datetime.now(),
                "source": "LOCAL"
            }

    def validate_batch(self, values: List[Any], context: Dict = None) -> List[Dict]:
        return [self.validate_single(v, context) for v in values]

    def get_cache_ttl(self) -> int:
        return 365 * 24 * 60 * 60  # 1 year (ISO codes rarely change)

    def get_rate_limit(self) -> Dict:
        return {
            "calls_per_minute": 10000,  # Local, no limit
            "calls_per_day": 10000000,
            "batch_size": 10000
        }


class INSEESiretProvider(ReferenceDataProvider):
    """
    Provider for validating French SIRET codes against INSEE Sirene API.

    SIRET: 14-digit French business establishment identifier
    API Documentation: https://api.insee.fr/catalogue/

    Requires:
    - INSEE API key and secret (OAuth2)
    - Free tier: 30 requests/minute, 1000/day
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
        if self._access_token and self._token_expiry and self._token_expiry > datetime.now():
            return self._access_token

        # Request new token
        auth_url = "https://api.insee.fr/token"
        response = requests.post(
            auth_url,
            auth=(self.api_key, self.api_secret),
            data={"grant_type": "client_credentials"},
            timeout=10
        )
        response.raise_for_status()

        token_data = response.json()
        self._access_token = token_data["access_token"]
        # Set expiry with 60 second buffer
        self._token_expiry = datetime.now() + timedelta(seconds=token_data.get("expires_in", 3600) - 60)

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
                raise Exception(f"API error: {response.status_code} - {response.text}")

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

        Note: INSEE API doesn't support batch queries natively.
        This implementation validates one at a time with rate limiting.
        """
        import time
        results = []
        rate_limit = self.get_rate_limit()
        delay = 60.0 / rate_limit["calls_per_minute"]  # Seconds between calls

        for value in values:
            result = self.validate_single(value, context)
            results.append(result)

            # Rate limiting
            if delay > 0:
                time.sleep(delay)

        return results

    def get_cache_ttl(self) -> int:
        """Cache for 30 days (company info changes infrequently)"""
        return 30 * 24 * 60 * 60  # 30 days in seconds

    def get_rate_limit(self) -> Dict:
        return {
            "calls_per_minute": 30,  # INSEE free tier limit
            "calls_per_day": 1000,
            "batch_size": 1  # No native batch support
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

        try:
            return luhn_checksum(siret) == 0
        except:
            return False

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


# ============================================================================
# Helper Functions
# ============================================================================

def register_insee_provider(api_key: str, api_secret: str):
    """
    Register INSEE SIRET provider with credentials.

    Usage:
        register_insee_provider(
            api_key="your_insee_api_key",
            api_secret="your_insee_api_secret"
        )
    """
    registry = get_reference_registry()
    provider = INSEESiretProvider(api_key, api_secret)
    registry.register(provider)


def get_provider_for_field(field_name: str) -> Optional[ReferenceDataProvider]:
    """
    Find the most appropriate provider for a field name.

    Returns the first matching provider or None.
    """
    registry = get_reference_registry()
    providers = registry.find_providers_for_field(field_name)
    return providers[0] if providers else None
