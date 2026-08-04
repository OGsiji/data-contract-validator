"""
Tests for HubSpot extractor.
"""

from unittest.mock import patch, Mock

from data_contract_validator.extractors.hubspot import HubSpotExtractor
from data_contract_validator.core.types import CanonicalType


def _property(
    name,
    type_="string",
    calculated=False,
    hidden=False,
    read_only_value=False,
):
    return {
        "name": name,
        "type": type_,
        "fieldType": "text",
        "calculated": calculated,
        "hidden": hidden,
        "modificationMetadata": {"readOnlyValue": read_only_value},
    }


class TestHubSpotExtractor:
    """Test the HubSpotExtractor class."""

    @patch("requests.get")
    def test_extracts_writable_properties_with_canonical_types(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                _property("email", "string"),
                _property("num_employees", "number"),
                _property("is_active", "bool"),
                _property("created_date", "datetime"),
            ]
        }
        mock_get.return_value = mock_response

        extractor = HubSpotExtractor(access_token="pat-fake", object_type="contacts")
        schemas = extractor.extract_schemas()

        assert "contacts" in schemas
        columns = {c["name"]: c for c in schemas["contacts"].columns}
        assert columns["email"]["canonical_type"] == CanonicalType.STRING.value
        assert columns["num_employees"]["canonical_type"] == CanonicalType.DECIMAL.value
        assert columns["is_active"]["canonical_type"] == CanonicalType.BOOLEAN.value
        assert columns["created_date"]["canonical_type"] == CanonicalType.TIMESTAMP.value

    @patch("requests.get")
    def test_unrecognized_type_resolves_to_unknown_not_a_guess(self, mock_get):
        """An unmapped HubSpot type (e.g. object_coordinates) must never be
        silently guessed into some canonical type -- that risks a false
        mismatch. It should resolve to UNKNOWN, same as the rest of the
        codebase's 'stay quiet rather than cry wolf' rule."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [_property("hq_coordinates", "object_coordinates")]
        }
        mock_get.return_value = mock_response

        extractor = HubSpotExtractor(access_token="pat-fake", object_type="companies")
        schemas = extractor.extract_schemas()

        col = schemas["companies"].columns[0]
        assert col["canonical_type"] == CanonicalType.UNKNOWN.value

    @patch("requests.get")
    def test_calculated_hidden_and_readonly_properties_are_excluded(self, mock_get):
        """None of these can actually be populated by a sync -- including
        them would produce a permanent, unfixable 'missing column'."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                _property("email", "string"),
                _property("hs_analytics_score", "number", calculated=True),
                _property("hs_internal_flag", "bool", hidden=True),
                _property("createdate", "datetime", read_only_value=True),
            ]
        }
        mock_get.return_value = mock_response

        extractor = HubSpotExtractor(access_token="pat-fake", object_type="contacts")
        schemas = extractor.extract_schemas()

        names = {c["name"] for c in schemas["contacts"].columns}
        assert names == {"email"}

    @patch("requests.get")
    def test_explicit_fields_list_scopes_to_only_those_properties(self, mock_get):
        """Without scoping, a real object's dozens of writable-but-unrelated
        properties would drown out the fields an actual sync cares about."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                _property("email", "string"),
                _property("phone", "string"),
                _property("lifecyclestage", "enumeration"),
            ]
        }
        mock_get.return_value = mock_response

        extractor = HubSpotExtractor(
            access_token="pat-fake", object_type="contacts", fields=["email"]
        )
        schemas = extractor.extract_schemas()

        names = {c["name"] for c in schemas["contacts"].columns}
        assert names == {"email"}

    @patch("requests.get")
    def test_requested_field_not_writable_is_skipped_not_crashed(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                _property("email", "string"),
                _property("hs_analytics_score", "number", calculated=True),
            ]
        }
        mock_get.return_value = mock_response

        extractor = HubSpotExtractor(
            access_token="pat-fake",
            object_type="contacts",
            fields=["email", "hs_analytics_score", "does_not_exist"],
        )
        schemas = extractor.extract_schemas()

        names = {c["name"] for c in schemas["contacts"].columns}
        assert names == {"email"}

    @patch("requests.get")
    def test_api_error_returns_empty_schema_dict(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_get.return_value = mock_response

        extractor = HubSpotExtractor(access_token="pat-fake", object_type="contacts")
        schemas = extractor.extract_schemas()

        assert schemas == {}

    @patch("requests.get")
    def test_sends_bearer_auth_header(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": [_property("email")]}
        mock_get.return_value = mock_response

        HubSpotExtractor(access_token="pat-secret-value", object_type="contacts").extract_schemas()

        _, kwargs = mock_get.call_args
        assert kwargs["headers"]["Authorization"] == "Bearer pat-secret-value"
