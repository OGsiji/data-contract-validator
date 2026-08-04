# data_contract_validator/extractors/hubspot.py
"""
HubSpot CRM schema extractor.

Reads live property (field) metadata for a CRM object via HubSpot's
Properties API -- this is a reverse-ETL *destination*, not a codebase, so
unlike FastAPI/Pydantic there's no source file to parse: the schema only
exists as whatever an admin has configured in the HubSpot UI, which can
change with no code review and no git history at all.

Two things make this a different shape of "target" than FastAPI:

1. A single CRM object routinely has 100-400+ properties, almost all
   irrelevant to any one sync. Diffing against the *entire* object would
   reintroduce exactly the false-positive noise this project's canonical
   type system was built to eliminate. So callers should pass an explicit
   ``fields`` allowlist scoping this to what's actually synced -- omitting
   it falls back to "every writable property," which is provided for
   exploration but is not the recommended steady-state config.
2. Most properties on a real object cannot be written to by a sync at all:
   HubSpot marks calculated/rollup properties and read-only properties in
   the metadata itself. Those are filtered out unconditionally -- including
   them would produce a permanent, unfixable "missing column" for a field
   nothing could ever populate.
"""

import requests
from typing import Dict, List, Optional

from .base import BaseExtractor
from ..core.models import Schema
from ..core.types import CanonicalType

# HubSpot's `type` field (not `fieldType`, which is just the UI widget) ->
# canonical type. HubSpot doesn't distinguish integer vs. floating-point at
# the schema level -- "number" covers both plain counts and calculated
# averages -- so it maps to the wider DECIMAL rank rather than INTEGER,
# consistent with how the Python `int` mapping already favors avoiding a
# false "narrower type" mismatch over precision we can't verify anyway.
_HUBSPOT_TYPE_MAP = {
    "string": CanonicalType.STRING,
    "phone_number": CanonicalType.STRING,
    "enumeration": CanonicalType.STRING,  # single/multi-select serialize as string(s)
    "number": CanonicalType.DECIMAL,
    "bool": CanonicalType.BOOLEAN,
    "datetime": CanonicalType.TIMESTAMP,
    "date": CanonicalType.DATE,
}


class HubSpotExtractor(BaseExtractor):
    """Extract a target schema from a live HubSpot CRM object's properties."""

    API_BASE = "https://api.hubapi.com"

    def __init__(
        self,
        access_token: str,
        object_type: str,
        fields: Optional[List[str]] = None,
    ):
        """
        Args:
            access_token: A HubSpot Private App access token (``pat-...``),
                scoped with read access to the object's schema
                (``crm.schemas.<object>.read``). Never a legacy API key --
                HubSpot no longer accepts those on this endpoint.
            object_type: The CRM object to read, e.g. ``"contacts"``,
                ``"companies"``, ``"deals"``, or a custom object's internal
                name.
            fields: The property (API) names actually populated by your
                sync. Strongly recommended -- without it, every writable
                property on the object is used, which for a stock ``contacts``
                object is still dozens of fields unrelated to any sync you
                run, and will produce warnings for all of them.
        """
        self.access_token = access_token
        self.object_type = object_type
        self.fields = set(fields) if fields else None

    def extract_schemas(self) -> Dict[str, Schema]:
        print(f"🔍 Extracting HubSpot schema for object '{self.object_type}'")

        response = requests.get(
            f"{self.API_BASE}/crm/v3/properties/{self.object_type}",
            headers={"Authorization": f"Bearer {self.access_token}"},
        )
        if response.status_code != 200:
            print(
                f"   ❌ HubSpot API error {response.status_code} for "
                f"'{self.object_type}': {response.text[:200]}"
            )
            return {}

        properties = response.json().get("results", [])
        writable = {
            prop["name"]: prop
            for prop in properties
            if not prop.get("calculated")
            and not prop.get("hidden")
            and not prop.get("modificationMetadata", {}).get("readOnlyValue")
        }

        if self.fields is None:
            print(
                f"   ⚠️  No explicit 'fields' list given -- comparing against "
                f"all {len(writable)} writable properties on '{self.object_type}'. "
                f"Pass the fields your sync actually populates to avoid noise "
                f"from unrelated CRM fields."
            )
            selected = writable
        else:
            selected = {}
            for name in self.fields:
                if name in writable:
                    selected[name] = writable[name]
                else:
                    reason = "not found" if name not in {
                        p["name"] for p in properties
                    } else "not writable (calculated, hidden, or read-only)"
                    print(f"   ⚠️  Requested field '{name}' is {reason} -- skipping")

        columns = [
            self._make_column(
                name,
                raw_type=prop["type"],
                canonical_type=_HUBSPOT_TYPE_MAP.get(prop["type"], CanonicalType.UNKNOWN),
                required=False,  # HubSpot properties have no schema-level "required"
                nullable=True,
            )
            for name, prop in selected.items()
        ]

        if not columns:
            print(f"   ❌ No usable properties found for '{self.object_type}'")
            return {}

        print(f"   ✅ Found {len(columns)} properties")
        return {
            self.object_type: Schema(
                name=self.object_type,
                columns=columns,
                source=f"hubspot:{self.object_type}",
                metadata={"confidence": "high", "complete": True},
            )
        }
