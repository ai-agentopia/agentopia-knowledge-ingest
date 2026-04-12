"""Targeted tests for GET /config/google-picker endpoint.

Validates the readiness contract:
  - empty values       → configured=false, empty strings returned
  - whitespace-only    → configured=false
  - PLACEHOLDER (any case) → configured=false
  - real values        → configured=true, values returned as-is

These tests directly exercise the HTTP endpoint via TestClient so they
cover the full request/response path including the _real() filter in
config_routes.py.
"""

import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from fastapi.testclient import TestClient
import main  # noqa — registers all routers including config_router
from main import app

CLIENT = TestClient(app, raise_server_exceptions=True)
ENDPOINT = "/config/google-picker"


class TestPickerConfigEmptyValues(unittest.TestCase):
    """configured=false when env vars are absent or empty."""

    def _get(self, api_key="", client_id="", app_id=""):
        env = {
            "GOOGLE_PICKER_API_KEY": api_key,
            "GOOGLE_PICKER_CLIENT_ID": client_id,
            "GOOGLE_PICKER_APP_ID": app_id,
        }
        with patch.dict(os.environ, env):
            # Bust the settings cache so patched env is picked up
            import config as cfg
            cfg.get_settings.cache_clear()
            resp = CLIENT.get(ENDPOINT)
        cfg.get_settings.cache_clear()
        return resp

    def test_all_empty_returns_configured_false(self):
        resp = self._get("", "", "")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertFalse(body["configured"])
        self.assertEqual(body["api_key"], "")
        self.assertEqual(body["client_id"], "")

    def test_api_key_missing_client_id_present_returns_false(self):
        resp = self._get("", "real-client-id", "")
        self.assertFalse(resp.json()["configured"])

    def test_client_id_missing_api_key_present_returns_false(self):
        resp = self._get("real-api-key", "", "")
        self.assertFalse(resp.json()["configured"])

    def test_whitespace_only_values_return_configured_false(self):
        resp = self._get("   ", "   ", "")
        body = resp.json()
        self.assertFalse(body["configured"])
        # Whitespace is stripped and not forwarded to browser
        self.assertEqual(body["api_key"], "")
        self.assertEqual(body["client_id"], "")


class TestPickerConfigPlaceholderValues(unittest.TestCase):
    """configured=false for any case variant of the literal string PLACEHOLDER."""

    def _get(self, api_key, client_id):
        env = {
            "GOOGLE_PICKER_API_KEY": api_key,
            "GOOGLE_PICKER_CLIENT_ID": client_id,
            "GOOGLE_PICKER_APP_ID": "",
        }
        with patch.dict(os.environ, env):
            import config as cfg
            cfg.get_settings.cache_clear()
            resp = CLIENT.get(ENDPOINT)
        cfg.get_settings.cache_clear()
        return resp

    def test_uppercase_placeholder_returns_configured_false(self):
        resp = self._get("PLACEHOLDER", "PLACEHOLDER")
        body = resp.json()
        self.assertFalse(body["configured"])
        # Placeholder sentinel must not be forwarded to browser
        self.assertEqual(body["api_key"], "")
        self.assertEqual(body["client_id"], "")

    def test_lowercase_placeholder_returns_configured_false(self):
        resp = self._get("placeholder", "placeholder")
        self.assertFalse(resp.json()["configured"])

    def test_mixed_case_placeholder_returns_configured_false(self):
        resp = self._get("Placeholder", "PlaceHolder")
        self.assertFalse(resp.json()["configured"])

    def test_placeholder_with_surrounding_whitespace_returns_false(self):
        resp = self._get("  PLACEHOLDER  ", "  PLACEHOLDER  ")
        body = resp.json()
        self.assertFalse(body["configured"])
        self.assertEqual(body["api_key"], "")

    def test_only_api_key_placeholder_returns_false(self):
        resp = self._get("PLACEHOLDER", "real-client-id")
        self.assertFalse(resp.json()["configured"])

    def test_only_client_id_placeholder_returns_false(self):
        resp = self._get("real-api-key", "PLACEHOLDER")
        self.assertFalse(resp.json()["configured"])


class TestPickerConfigRealValues(unittest.TestCase):
    """configured=true and values returned when both real values are present."""

    def _get(self, api_key, client_id, app_id=""):
        env = {
            "GOOGLE_PICKER_API_KEY": api_key,
            "GOOGLE_PICKER_CLIENT_ID": client_id,
            "GOOGLE_PICKER_APP_ID": app_id,
        }
        with patch.dict(os.environ, env):
            import config as cfg
            cfg.get_settings.cache_clear()
            resp = CLIENT.get(ENDPOINT)
        cfg.get_settings.cache_clear()
        return resp

    def test_real_values_return_configured_true(self):
        resp = self._get("AIzaSy-real-api-key", "123456-clientid.apps.googleusercontent.com")
        body = resp.json()
        self.assertTrue(body["configured"])
        self.assertEqual(body["api_key"], "AIzaSy-real-api-key")
        self.assertEqual(body["client_id"], "123456-clientid.apps.googleusercontent.com")

    def test_real_values_with_app_id_returns_configured_true(self):
        resp = self._get(
            "AIzaSy-real-api-key",
            "123456-clientid.apps.googleusercontent.com",
            "my-project-123",
        )
        body = resp.json()
        self.assertTrue(body["configured"])
        self.assertEqual(body["app_id"], "my-project-123")

    def test_real_values_not_mangled(self):
        """Values that contain the word placeholder elsewhere are not stripped."""
        resp = self._get("AIzaSy-placeholder-lookup", "real-client-123")
        body = resp.json()
        # "placeholder" appears as a substring but the full value is not == "PLACEHOLDER"
        self.assertTrue(body["configured"])
        self.assertEqual(body["api_key"], "AIzaSy-placeholder-lookup")

    def test_response_schema_always_present(self):
        """Response always includes all four fields regardless of configured state."""
        resp = self._get("", "")
        body = resp.json()
        self.assertIn("api_key", body)
        self.assertIn("client_id", body)
        self.assertIn("app_id", body)
        self.assertIn("configured", body)


if __name__ == "__main__":
    unittest.main()
