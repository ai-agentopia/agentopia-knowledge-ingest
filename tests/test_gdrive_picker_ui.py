"""W-C3.4 Google Drive picker UI tests — knowledge-ingest#48.

Two layers of validation:

Layer 1 — HTML/JS string checks (classes TestGDriveSectionPresent through
TestGDriveButtonWiring): validate structure, identity contract, and error-handling
text in the rendered HTML. Fast, no external dependencies.

Layer 2 — Node.js execution (class TestGDriveJSExecution): extracts the <script>
block from /ui, prepends minimal DOM/Google-API stubs, and runs via Node.js
subprocess. This exercises actual JS runtime behaviour — format inference, Sheets/
Slides rejection logic, source_uri population in the picker callback, and
_arrayBufferToBase64 encoding — none of which are string-matchable with confidence.
Requires Node.js >= 16 (v22 available in this environment).

Coverage:
  1. Google Drive section renders in /ui response.
  2. Config warning shown + picker button disabled when env vars not set.
  3. Config warning absent + picker button enabled when env vars are set.
  4. connector_module is exactly 'google_drive' in JS payload.
  5. source_uri format 'gdrive://' present in JS.
  6. Submit path targets POST /connectors/ingest.
  7. raw_bytes_b64 present in payload construction.
  8. Google Sheets and Google Slides rejection text present (string) and executed (Node.js).
  9. 422 and 413 response handling present in JS.
 10. Verdict display (fetched_new, skipped_unchanged, fetch_failed) in JS.
 11. No new Python routes added to connector_routes.py or s3_routes.py.
 12. No new Python module imports added to routes.py beyond the allowed set.
 13. _GDRIVE_CFG injected by operator_ui() when env vars are set.
 14. Picker button id and ingest button id exist in DOM.
 15. [Node.js] _gdriveInferFormat returns correct format for PDF, DOCX, HTML, Google Docs.
 16. [Node.js] _gdriveInferFormat returns null for Sheets/Slides.
 17. [Node.js] _gdrivePickerCallback rejects Sheets — sets message, leaves _gdriveSelectedFile null.
 18. [Node.js] _gdrivePickerCallback populates source_uri, filename, format for PDF file.
 19. [Node.js] _arrayBufferToBase64 encodes known bytes to correct base64 string.
 20. [Node.js] _gdriveConfigured() returns true/false based on _GDRIVE_CFG values.
"""

import contextlib
import importlib
import json
import os
import shutil
import subprocess
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from fastapi.testclient import TestClient
import main  # noqa  — registers all routers
from main import app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_ui_html(env_overrides: dict | None = None) -> str:
    """GET /ui with optional env-var overrides injected via config mock."""
    env_overrides = env_overrides or {}

    with patch.dict(os.environ, env_overrides, clear=False):
        # Force config to re-read env by bypassing lru_cache
        import config as cfg_module
        cfg_module.get_settings.cache_clear()
        try:
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/ui")
            assert resp.status_code == 200, f"/ui returned {resp.status_code}"
            return resp.text
        finally:
            cfg_module.get_settings.cache_clear()


def _unconfigured_html() -> str:
    """Render /ui without picker env vars (simulates unconfigured state)."""
    env = {
        "GOOGLE_PICKER_API_KEY": "",
        "GOOGLE_PICKER_CLIENT_ID": "",
        "GOOGLE_PICKER_APP_ID": "",
    }
    return _get_ui_html(env)


def _configured_html() -> str:
    """Render /ui with picker env vars set (simulates configured state)."""
    env = {
        "GOOGLE_PICKER_API_KEY": "test-api-key-abc",
        "GOOGLE_PICKER_CLIENT_ID": "test-client-id-xyz.apps.googleusercontent.com",
        "GOOGLE_PICKER_APP_ID": "123456789",
    }
    return _get_ui_html(env)


# ---------------------------------------------------------------------------
# 1. Section presence
# ---------------------------------------------------------------------------

class TestGDriveSectionPresent(unittest.TestCase):
    """The Google Drive section must exist in the rendered /ui page."""

    def test_gdrive_section_heading_exists(self):
        """hGDrive heading element must exist in the DOM."""
        html = _unconfigured_html()
        self.assertIn('id="hGDrive"', html,
                      "Google Drive section heading (hGDrive) not found in /ui")

    def test_gdrive_section_body_exists(self):
        """sGDrive section body must exist in the DOM."""
        html = _unconfigured_html()
        self.assertIn('id="sGDrive"', html,
                      "Google Drive section body (sGDrive) not found in /ui")

    def test_gdrive_section_heading_text(self):
        """Section heading must identify as Google Drive Ingest."""
        html = _unconfigured_html()
        self.assertIn("Google Drive Ingest", html,
                      "Section heading must contain 'Google Drive Ingest'")

    def test_gdrive_source_uri_field_exists(self):
        """gdriveSourceUri input must exist so source_uri is visible."""
        html = _unconfigured_html()
        self.assertIn('id="gdriveSourceUri"', html,
                      "gdriveSourceUri input missing from /ui")

    def test_gdrive_filename_field_exists(self):
        """gdriveFileName input must exist for selected file display."""
        html = _unconfigured_html()
        self.assertIn('id="gdriveFileName"', html,
                      "gdriveFileName input missing from /ui")

    def test_gdrive_format_field_exists(self):
        """gdriveFormat input must exist for inferred format display."""
        html = _unconfigured_html()
        self.assertIn('id="gdriveFormat"', html,
                      "gdriveFormat input missing from /ui")

    def test_gdrive_scope_field_exists(self):
        """gdriveScope input must exist for scope entry."""
        html = _unconfigured_html()
        self.assertIn('id="gdriveScope"', html,
                      "gdriveScope input missing from /ui")

    def test_gdrive_output_element_exists(self):
        """gdriveOut output element must exist for status/result display."""
        html = _unconfigured_html()
        self.assertIn('id="gdriveOut"', html,
                      "gdriveOut element missing from /ui")


# ---------------------------------------------------------------------------
# 2. Unconfigured state — warning shown, button disabled
# ---------------------------------------------------------------------------

class TestGDriveUnconfiguredState(unittest.TestCase):
    """When picker env vars are absent, the UI must show a config warning."""

    def test_config_warning_element_exists(self):
        """gdriveConfigWarning element must exist in DOM."""
        html = _unconfigured_html()
        self.assertIn('id="gdriveConfigWarning"', html,
                      "gdriveConfigWarning element missing from /ui")

    def test_config_warning_mentions_env_vars(self):
        """Warning text must name the required env vars."""
        html = _unconfigured_html()
        self.assertIn("GOOGLE_PICKER_API_KEY", html,
                      "Config warning must name GOOGLE_PICKER_API_KEY")
        self.assertIn("GOOGLE_PICKER_CLIENT_ID", html,
                      "Config warning must name GOOGLE_PICKER_CLIENT_ID")

    def test_pick_button_exists(self):
        """gdriveBtnPick button must exist in DOM."""
        html = _unconfigured_html()
        self.assertIn('id="gdriveBtnPick"', html,
                      "gdriveBtnPick button missing from /ui")

    def test_ingest_button_starts_disabled(self):
        """gdriveBtnIngest must start disabled before a file is selected."""
        html = _unconfigured_html()
        self.assertIn('id="gdriveBtnIngest"', html,
                      "gdriveBtnIngest button missing from /ui")
        # The button must have disabled attribute or disabled in HTML
        idx = html.find('id="gdriveBtnIngest"')
        surroundings = html[max(0, idx - 50):idx + 200]
        self.assertIn("disabled", surroundings,
                      "gdriveBtnIngest must be disabled by default")

    def test_gdrive_cfg_empty_when_unconfigured(self):
        """_GDRIVE_CFG must inject empty strings when env vars unset."""
        html = _unconfigured_html()
        # The injected config should contain empty apiKey
        self.assertIn('const _GDRIVE_CFG = {', html,
                      "_GDRIVE_CFG injection missing from /ui script block")
        self.assertIn('apiKey:""', html,
                      "apiKey must be empty string when GOOGLE_PICKER_API_KEY is unset")
        self.assertIn('clientId:""', html,
                      "clientId must be empty string when GOOGLE_PICKER_CLIENT_ID is unset")

    def test_init_gdrive_config_state_iife_present(self):
        """_initGDriveConfigState IIFE must be present to disable button on load."""
        html = _unconfigured_html()
        self.assertIn("_initGDriveConfigState", html,
                      "_initGDriveConfigState IIFE missing — button won't disable when unconfigured")

    def test_gdrive_configured_check_function_present(self):
        """_gdriveConfigured() function must exist."""
        html = _unconfigured_html()
        self.assertIn("function _gdriveConfigured", html,
                      "_gdriveConfigured function missing from /ui JS")


# ---------------------------------------------------------------------------
# 3. Configured state — env vars injected correctly
# ---------------------------------------------------------------------------

class TestGDriveConfiguredState(unittest.TestCase):
    """When picker env vars are set, the injected JS config must carry their values."""

    def test_api_key_injected(self):
        """apiKey in _GDRIVE_CFG must match GOOGLE_PICKER_API_KEY env var."""
        html = _configured_html()
        self.assertIn('"test-api-key-abc"', html,
                      "GOOGLE_PICKER_API_KEY value not injected into _GDRIVE_CFG.apiKey")

    def test_client_id_injected(self):
        """clientId in _GDRIVE_CFG must match GOOGLE_PICKER_CLIENT_ID env var."""
        html = _configured_html()
        self.assertIn('"test-client-id-xyz.apps.googleusercontent.com"', html,
                      "GOOGLE_PICKER_CLIENT_ID value not injected into _GDRIVE_CFG.clientId")

    def test_app_id_injected(self):
        """appId in _GDRIVE_CFG must match GOOGLE_PICKER_APP_ID env var."""
        html = _configured_html()
        self.assertIn('"123456789"', html,
                      "GOOGLE_PICKER_APP_ID value not injected into _GDRIVE_CFG.appId")

    def test_gdrive_cfg_structure_well_formed(self):
        """_GDRIVE_CFG must be a JS object with apiKey, clientId, appId keys."""
        html = _configured_html()
        cfg_start = html.find("const _GDRIVE_CFG = {")
        self.assertGreater(cfg_start, -1, "_GDRIVE_CFG declaration not found")
        cfg_region = html[cfg_start:cfg_start + 300]
        self.assertIn("apiKey:", cfg_region)
        self.assertIn("clientId:", cfg_region)
        self.assertIn("appId:", cfg_region)


# ---------------------------------------------------------------------------
# 4–6. Identity contract: connector_module, source_uri, submit path
# ---------------------------------------------------------------------------

class TestGDriveIdentityContract(unittest.TestCase):
    """JS must implement the identity contract from knowledge-ingest#38/#48."""

    def test_connector_module_is_google_drive(self):
        """connector_module must be exactly 'google_drive' in the payload JS."""
        html = _unconfigured_html()
        # Look in the doGDriveIngest function body
        start = html.find("async function doGDriveIngest")
        self.assertGreater(start, -1, "doGDriveIngest function not found")
        fn_body = html[start:start + 2000]
        self.assertIn("connector_module: 'google_drive'", fn_body,
                      "connector_module must be 'google_drive' in doGDriveIngest payload")

    def test_source_uri_gdrive_scheme(self):
        """source_uri must use gdrive:// scheme in payload and UI."""
        html = _unconfigured_html()
        self.assertIn("gdrive://", html,
                      "gdrive:// URI scheme must appear in /ui (identity contract)")

    def test_source_uri_built_from_file_id_in_payload(self):
        """source_uri in payload must be constructed as 'gdrive://' + fileId."""
        html = _unconfigured_html()
        start = html.find("async function doGDriveIngest")
        fn_body = html[start:start + 2000]
        self.assertIn("'gdrive://' + fileId", fn_body,
                      "source_uri must be built as 'gdrive://' + fileId in doGDriveIngest")

    def test_source_uri_in_picker_callback(self):
        """_gdrivePickerCallback must set gdriveSourceUri to gdrive:// + fileId."""
        html = _unconfigured_html()
        start = html.find("function _gdrivePickerCallback")
        self.assertGreater(start, -1, "_gdrivePickerCallback not found")
        fn_body = html[start:start + 1500]
        self.assertIn("gdrive://", fn_body,
                      "Picker callback must write gdrive:// URI to gdriveSourceUri field")

    def test_submit_path_is_connectors_ingest(self):
        """doGDriveIngest must POST to '/connectors/ingest'."""
        html = _unconfigured_html()
        start = html.find("async function doGDriveIngest")
        fn_body = html[start:start + 2000]
        self.assertIn("'/connectors/ingest'", fn_body,
                      "Submit path must be '/connectors/ingest' (W-C3.1 endpoint)")

    def test_method_is_post(self):
        """doGDriveIngest must use HTTP POST."""
        html = _unconfigured_html()
        start = html.find("async function doGDriveIngest")
        fn_body = html[start:start + 2000]
        self.assertIn("method: 'POST'", fn_body,
                      "doGDriveIngest must use POST method")


# ---------------------------------------------------------------------------
# 7. raw_bytes_b64 payload field
# ---------------------------------------------------------------------------

class TestGDrivePayloadFields(unittest.TestCase):
    """The payload submitted to /connectors/ingest must include raw_bytes_b64."""

    def test_raw_bytes_b64_in_payload(self):
        """raw_bytes_b64 must be a field in the doGDriveIngest payload object."""
        html = _unconfigured_html()
        start = html.find("async function doGDriveIngest")
        fn_body = html[start:start + 2000]
        self.assertIn("raw_bytes_b64", fn_body,
                      "raw_bytes_b64 must be in the payload sent to /connectors/ingest")

    def test_array_buffer_to_base64_function_present(self):
        """_arrayBufferToBase64 helper must exist for encoding file bytes."""
        html = _unconfigured_html()
        self.assertIn("function _arrayBufferToBase64", html,
                      "_arrayBufferToBase64 function missing from /ui JS")

    def test_base64_used_before_submission(self):
        """doGDriveIngest must call _arrayBufferToBase64 to encode bytes."""
        html = _unconfigured_html()
        start = html.find("async function doGDriveIngest")
        fn_body = html[start:start + 2000]
        self.assertIn("_arrayBufferToBase64", fn_body,
                      "doGDriveIngest must call _arrayBufferToBase64 before submitting")

    def test_payload_includes_scope_and_filename(self):
        """Payload must include scope and filename fields."""
        html = _unconfigured_html()
        start = html.find("async function doGDriveIngest")
        fn_body = html[start:start + 2000]
        self.assertIn("scope", fn_body,
                      "Payload must include scope")
        self.assertIn("filename", fn_body,
                      "Payload must include filename")


# ---------------------------------------------------------------------------
# 8. Sheets/Slides rejection
# ---------------------------------------------------------------------------

class TestGDriveSheetsSlideRejection(unittest.TestCase):
    """Google Sheets and Google Slides must be rejected at picker selection."""

    def test_unsupported_workspace_set_defined(self):
        """_GDRIVE_UNSUPPORTED_WORKSPACE must be a Set containing Sheets and Slides mimes."""
        html = _unconfigured_html()
        self.assertIn("_GDRIVE_UNSUPPORTED_WORKSPACE", html,
                      "_GDRIVE_UNSUPPORTED_WORKSPACE not defined in /ui JS")

    def test_sheets_mime_in_unsupported_set(self):
        """Google Sheets MIME type must be in _GDRIVE_UNSUPPORTED_WORKSPACE."""
        html = _unconfigured_html()
        self.assertIn("application/vnd.google-apps.spreadsheet", html,
                      "Sheets MIME must be in unsupported workspace set")

    def test_slides_mime_in_unsupported_set(self):
        """Google Slides MIME type must be in _GDRIVE_UNSUPPORTED_WORKSPACE."""
        html = _unconfigured_html()
        self.assertIn("application/vnd.google-apps.presentation", html,
                      "Slides MIME must be in unsupported workspace set")

    def test_picker_callback_checks_unsupported_workspace(self):
        """_gdrivePickerCallback must check _GDRIVE_UNSUPPORTED_WORKSPACE."""
        html = _unconfigured_html()
        start = html.find("function _gdrivePickerCallback")
        self.assertGreater(start, -1, "_gdrivePickerCallback not found")
        fn_body = html[start:start + 1500]
        self.assertIn("_GDRIVE_UNSUPPORTED_WORKSPACE", fn_body,
                      "Picker callback must check _GDRIVE_UNSUPPORTED_WORKSPACE to reject Sheets/Slides")

    def test_sheets_rejected_with_label(self):
        """Rejection message must name 'Google Sheets' for spreadsheet MIME."""
        html = _unconfigured_html()
        self.assertIn("Google Sheets", html,
                      "Rejection message must name 'Google Sheets'")

    def test_slides_rejected_with_label(self):
        """Rejection message must name 'Google Slides' for presentation MIME."""
        html = _unconfigured_html()
        self.assertIn("Google Slides", html,
                      "Rejection message must name 'Google Slides'")

    def test_rejection_disables_ingest_button(self):
        """Rejection path must disable gdriveBtnIngest."""
        html = _unconfigured_html()
        start = html.find("function _gdrivePickerCallback")
        fn_body = html[start:start + 1500]
        # After the unsupported-workspace block, ingest button must be re-disabled
        self.assertIn("gdriveBtnIngest", fn_body,
                      "Rejection must reference gdriveBtnIngest (to disable it)")

    def test_ui_prose_explains_rejection(self):
        """UI prose text must state that Sheets/Slides are rejected at selection."""
        html = _unconfigured_html()
        self.assertIn("Google Sheets and Google Slides are rejected", html,
                      "UI must explain Sheets/Slides rejection in the description text")


# ---------------------------------------------------------------------------
# 9. Response handling: 422, 413, verdicts
# ---------------------------------------------------------------------------

class TestGDriveResponseHandling(unittest.TestCase):
    """doGDriveIngest must handle all documented response codes and verdicts."""

    def _ingest_fn_body(self) -> str:
        html = _unconfigured_html()
        start = html.find("async function doGDriveIngest")
        self.assertGreater(start, -1, "doGDriveIngest not found")
        return html[start:start + 5000]

    def test_handles_413_response(self):
        """doGDriveIngest must detect and report 413 (file too large)."""
        fn_body = self._ingest_fn_body()
        self.assertIn("413", fn_body,
                      "doGDriveIngest must handle HTTP 413 (payload too large)")

    def test_413_message_mentions_limit(self):
        """413 message must mention the size limit (50 MiB)."""
        fn_body = self._ingest_fn_body()
        self.assertIn("50 MiB", fn_body,
                      "413 error message must state the 50 MiB limit")

    def test_handles_422_response(self):
        """doGDriveIngest must detect and report 422 (validation error)."""
        fn_body = self._ingest_fn_body()
        self.assertIn("422", fn_body,
                      "doGDriveIngest must handle HTTP 422 (validation error)")

    def test_handles_success_verdict_fetched_new(self):
        """Verdict 'fetched_new' must appear in the verdict colour map."""
        fn_body = self._ingest_fn_body()
        self.assertIn("fetched_new", fn_body,
                      "fetched_new verdict must be handled in doGDriveIngest")

    def test_handles_verdict_skipped_unchanged(self):
        """Verdict 'skipped_unchanged' must appear in the verdict colour map."""
        fn_body = self._ingest_fn_body()
        self.assertIn("skipped_unchanged", fn_body,
                      "skipped_unchanged verdict must be handled in doGDriveIngest")

    def test_handles_verdict_fetch_failed(self):
        """Verdict 'fetch_failed' must appear in the verdict colour map."""
        fn_body = self._ingest_fn_body()
        self.assertIn("fetch_failed", fn_body,
                      "fetch_failed verdict must be handled in doGDriveIngest")

    def test_handles_verdict_fetched_updated(self):
        """Verdict 'fetched_updated' must appear in the verdict colour map."""
        fn_body = self._ingest_fn_body()
        self.assertIn("fetched_updated", fn_body,
                      "fetched_updated verdict must be handled in doGDriveIngest")

    def test_drive_api_error_handled(self):
        """Drive API fetch errors must be caught and reported."""
        fn_body = self._ingest_fn_body()
        self.assertIn("Drive API error", fn_body,
                      "Drive API HTTP errors must be surfaced to operator")

    def test_network_error_caught(self):
        """Network-level errors (fetch failure) must be caught."""
        fn_body = self._ingest_fn_body()
        self.assertIn("Network error", fn_body,
                      "Network errors must be caught and displayed in doGDriveIngest")


# ---------------------------------------------------------------------------
# 10. MIME type support maps
# ---------------------------------------------------------------------------

class TestGDriveMimeSupport(unittest.TestCase):
    """Supported and export MIME maps must be consistent with server-side #38 boundary."""

    def test_direct_map_includes_pdf(self):
        """_GDRIVE_DIRECT must include application/pdf → pdf."""
        html = _unconfigured_html()
        self.assertIn("'application/pdf': 'pdf'", html,
                      "_GDRIVE_DIRECT must map application/pdf to 'pdf'")

    def test_direct_map_includes_docx(self):
        """_GDRIVE_DIRECT must include OOXML DOCX MIME."""
        html = _unconfigured_html()
        self.assertIn("application/vnd.openxmlformats-officedocument.wordprocessingml.document", html,
                      "_GDRIVE_DIRECT must include DOCX MIME type")

    def test_direct_map_includes_html(self):
        """_GDRIVE_DIRECT must map text/html → html."""
        html = _unconfigured_html()
        self.assertIn("'text/html': 'html'", html,
                      "_GDRIVE_DIRECT must map text/html to 'html'")

    def test_export_map_google_docs_to_docx(self):
        """_GDRIVE_EXPORT must map Google Docs to DOCX export."""
        html = _unconfigured_html()
        self.assertIn("application/vnd.google-apps.document", html,
                      "_GDRIVE_EXPORT must include Google Docs MIME for DOCX export")

    def test_infer_format_function_present(self):
        """_gdriveInferFormat helper must be defined."""
        html = _unconfigured_html()
        self.assertIn("function _gdriveInferFormat", html,
                      "_gdriveInferFormat function missing from /ui JS")

    def test_supported_formats_listed_in_prose(self):
        """UI prose must list supported formats for operator guidance."""
        html = _unconfigured_html()
        self.assertIn("PDF", html)
        self.assertIn("DOCX", html)
        self.assertIn("Google Docs", html)


# ---------------------------------------------------------------------------
# 11–12. No new routes or modules added
# ---------------------------------------------------------------------------

class TestNoNewRoutesOrModules(unittest.TestCase):
    """W-C3.4 must not add new HTTP routes or Python modules."""

    def test_no_new_gdrive_route_in_connector_routes(self):
        """connector_routes.py must not have a dedicated /gdrive endpoint."""
        connector_routes_path = os.path.join(
            os.path.dirname(__file__), "..", "src", "api", "connector_routes.py"
        )
        with open(connector_routes_path) as f:
            content = f.read()
        # The only connector route added for W-C3.4 is browser-side — no new /gdrive HTTP route
        self.assertNotIn('"/connectors/gdrive"', content,
                         "connector_routes.py must not expose a /connectors/gdrive endpoint "
                         "(W-C3.4 is browser-side only)")
        self.assertNotIn("@router.post(\"/connectors/gdrive", content,
                         "connector_routes.py must not have a /connectors/gdrive POST route")

    def test_no_new_gdrive_route_in_s3_routes(self):
        """s3_routes.py must not have a /gdrive endpoint."""
        s3_routes_path = os.path.join(
            os.path.dirname(__file__), "..", "src", "api", "s3_routes.py"
        )
        if not os.path.exists(s3_routes_path):
            return  # s3_routes.py may not exist — pass
        with open(s3_routes_path) as f:
            content = f.read()
        self.assertNotIn("gdrive", content.lower(),
                         "s3_routes.py must not reference gdrive — wrong module")

    def test_routes_py_imports_no_new_gdrive_module(self):
        """routes.py must not import a new gdrive-specific Python module for W-C3.4."""
        routes_path = os.path.join(
            os.path.dirname(__file__), "..", "src", "api", "routes.py"
        )
        with open(routes_path) as f:
            content = f.read()
        # W-C3.4 must not import a new top-level module like 'google_drive_picker' or 'gdrive_ui'
        self.assertNotIn("import google_drive_picker", content,
                         "routes.py must not import a new google_drive_picker module (W-C3.4 is inline JS only)")
        self.assertNotIn("import gdrive_ui", content,
                         "routes.py must not import a new gdrive_ui module")

    def test_gdrive_config_in_settings(self):
        """Settings must expose picker config fields (google_picker_api_key etc.)."""
        from config import Settings
        s = Settings()
        # Fields must exist (may be empty strings in test env)
        self.assertTrue(hasattr(s, "google_picker_api_key"),
                        "Settings must have google_picker_api_key")
        self.assertTrue(hasattr(s, "google_picker_client_id"),
                        "Settings must have google_picker_client_id")
        self.assertTrue(hasattr(s, "google_picker_app_id"),
                        "Settings must have google_picker_app_id")


# ---------------------------------------------------------------------------
# 13. operator_ui() dynamic config injection
# ---------------------------------------------------------------------------

class TestOperatorUiDynamicConfig(unittest.TestCase):
    """operator_ui() must inject _GDRIVE_CFG from env vars on each request."""

    def test_placeholder_not_in_output(self):
        """The raw placeholder comment must not appear in rendered output."""
        html = _unconfigured_html()
        self.assertNotIn("/* GDRIVE_CONFIG_PLACEHOLDER */", html,
                         "Placeholder comment must be replaced by config injection")

    def test_gdrive_cfg_declaration_always_present(self):
        """_GDRIVE_CFG const must always be declared (even when empty)."""
        html = _unconfigured_html()
        self.assertIn("const _GDRIVE_CFG = {", html,
                      "_GDRIVE_CFG must always be declared in rendered /ui")

    def test_configured_values_distinct_from_empty(self):
        """Configured render must differ from unconfigured render at _GDRIVE_CFG."""
        unconfigured = _unconfigured_html()
        configured = _configured_html()
        # Both must have _GDRIVE_CFG, but values must differ
        self.assertIn("const _GDRIVE_CFG = {", unconfigured)
        self.assertIn("const _GDRIVE_CFG = {", configured)
        # The key difference: apiKey should be empty in one, non-empty in the other
        self.assertIn('apiKey:""', unconfigured)
        self.assertNotIn('apiKey:""', configured)


# ---------------------------------------------------------------------------
# 14. Button wiring
# ---------------------------------------------------------------------------

class TestGDriveButtonWiring(unittest.TestCase):
    """Picker and ingest buttons must be wired to the correct JS functions."""

    def test_pick_button_calls_doGDrivePick(self):
        """gdriveBtnPick onclick must call doGDrivePick()."""
        html = _unconfigured_html()
        idx = html.find('id="gdriveBtnPick"')
        surroundings = html[max(0, idx - 20):idx + 200]
        self.assertIn("doGDrivePick()", surroundings,
                      "gdriveBtnPick button must call doGDrivePick() on click")

    def test_ingest_button_calls_doGDriveIngest(self):
        """gdriveBtnIngest onclick must call doGDriveIngest()."""
        html = _unconfigured_html()
        idx = html.find('id="gdriveBtnIngest"')
        surroundings = html[max(0, idx - 20):idx + 250]
        self.assertIn("doGDriveIngest()", surroundings,
                      "gdriveBtnIngest button must call doGDriveIngest() on click")

    def test_do_gdrive_pick_function_defined(self):
        """doGDrivePick function must be defined in the script block."""
        html = _unconfigured_html()
        self.assertIn("function doGDrivePick()", html,
                      "doGDrivePick function missing from /ui JS")

    def test_do_gdrive_ingest_function_defined(self):
        """doGDriveIngest function must be defined in the script block."""
        html = _unconfigured_html()
        self.assertIn("async function doGDriveIngest()", html,
                      "doGDriveIngest function missing from /ui JS")

    def test_open_picker_function_defined(self):
        """_gdriveOpenPicker function must be defined (called after token obtained)."""
        html = _unconfigured_html()
        self.assertIn("function _gdriveOpenPicker()", html,
                      "_gdriveOpenPicker function missing from /ui JS")

    def test_picker_callback_function_defined(self):
        """_gdrivePickerCallback function must be defined."""
        html = _unconfigured_html()
        self.assertIn("function _gdrivePickerCallback(", html,
                      "_gdrivePickerCallback function missing from /ui JS")


# ---------------------------------------------------------------------------
# 15. Node.js executed JS tests (Layer 2 — genuine runtime validation)
# ---------------------------------------------------------------------------

_NODE_STUBS = """\
// Minimal stubs so the script block runs in Node.js without a real browser.
// These cover the IIFE (_initGDriveConfigState) and picker callback DOM refs.
const _elements = {
  gdriveOut:       { textContent: '', innerHTML: '', style: {} },
  gdriveBtnIngest: { disabled: true,  style: { opacity: '0.5' } },
  gdriveBtnPick:   { disabled: false, style: { opacity: '1'   }, title: '' },
  gdriveConfigWarning: { style: { display: 'none' } },
  gdriveFileName:  { value: '' },
  gdriveSourceUri: { value: '' },
  gdriveFormat:    { value: '' },
};
const document = {
  getElementById: (id) => _elements[id] || { value: '', style: {}, disabled: false, textContent: '' },
  createElement: (tag) => ({ src: '', onload: null, setAttribute: () => {} }),
  head: { appendChild: (el) => { if (el.onload) el.onload(); } },
  readyState: 'complete',
  addEventListener: () => {},
};
const google = {
  picker: {
    Action: { PICKED: 'picked', CANCEL: 'cancel' },
    DocsView: class {
      setIncludeFolders() { return this; }
      setSelectFolderEnabled() { return this; }
    },
    PickerBuilder: class {
      addView()         { return this; } setOAuthToken()   { return this; }
      setDeveloperKey() { return this; } setCallback()     { return this; }
      setAppId()        { return this; } build()           { return { setVisible: () => {} }; }
    },
    ViewId: { DOCS: 'docs' },
  },
  accounts: { oauth2: { initTokenClient: () => ({ requestAccessToken: () => {} }) } },
};
// btoa is natively available in Node >= 16.
"""


class TestGDriveJSExecution(unittest.TestCase):
    """Execute extracted JS functions in Node.js to validate runtime behaviour.

    These tests exercise actual JavaScript execution — not string matching.
    They cover:
    - Format inference (_gdriveInferFormat)
    - Unsupported MIME rejection (_GDRIVE_UNSUPPORTED_WORKSPACE)
    - Picker callback: Sheets/Slides rejection path
    - Picker callback: supported file → source_uri / filename / format population
    - Base64 encoding (_arrayBufferToBase64)
    - Config detection (_gdriveConfigured)
    """

    @classmethod
    def setUpClass(cls):
        cls._node = shutil.which('node')

    def _skip_if_no_node(self):
        if not self._node:
            self.skipTest("Node.js not available on this machine")

    def _extract_js(self) -> str:
        """Return the <script> block content from the rendered /ui HTML."""
        html = _configured_html()
        start = html.find('<script>')
        end = html.rfind('</script>')
        self.assertGreater(start, -1, "<script> block not found in /ui")
        return html[start + len('<script>'):end]

    def _run_js(self, assertion_code: str) -> dict:
        """Prepend stubs + UI script block, append assertion_code, run in Node.js."""
        self._skip_if_no_node()
        js_body = self._extract_js()
        full_script = _NODE_STUBS + js_body + "\n" + assertion_code
        result = subprocess.run(
            [self._node, '-e', full_script],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            self.fail(
                f"Node.js execution failed (exit {result.returncode}):\n"
                f"STDERR: {result.stderr}\n"
                f"STDOUT: {result.stdout}"
            )
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            self.fail(f"Node.js output is not valid JSON:\n{result.stdout}")

    # ── Format inference ────────────────────────────────────────────────────

    def test_infer_format_pdf(self):
        """_gdriveInferFormat('application/pdf') must return format='pdf', exportMime=null."""
        out = self._run_js(
            "const r = _gdriveInferFormat('application/pdf');\n"
            "console.log(JSON.stringify({ format: r ? r.format : null, exportMime: r ? r.exportMime : 'MISSING' }));"
        )
        self.assertEqual(out['format'], 'pdf')
        self.assertIsNone(out['exportMime'])

    def test_infer_format_html(self):
        """_gdriveInferFormat('text/html') must return format='html'."""
        out = self._run_js(
            "const r = _gdriveInferFormat('text/html');\n"
            "console.log(JSON.stringify({ format: r ? r.format : null }));"
        )
        self.assertEqual(out['format'], 'html')

    def test_infer_format_plain_text(self):
        """_gdriveInferFormat('text/plain') must return format='txt'."""
        out = self._run_js(
            "const r = _gdriveInferFormat('text/plain');\n"
            "console.log(JSON.stringify({ format: r ? r.format : null }));"
        )
        self.assertEqual(out['format'], 'txt')

    def test_infer_format_google_docs_exports_docx(self):
        """Google Docs MIME must map to format='docx' with a non-null exportMime."""
        out = self._run_js(
            "const r = _gdriveInferFormat('application/vnd.google-apps.document');\n"
            "console.log(JSON.stringify({ format: r ? r.format : null, hasExportMime: r ? !!r.exportMime : false }));"
        )
        self.assertEqual(out['format'], 'docx')
        self.assertTrue(out['hasExportMime'], "Google Docs must have a non-null exportMime (DOCX export endpoint)")

    def test_infer_format_sheets_returns_null(self):
        """_gdriveInferFormat must return null for Google Sheets (unsupported)."""
        out = self._run_js(
            "const r = _gdriveInferFormat('application/vnd.google-apps.spreadsheet');\n"
            "console.log(JSON.stringify({ result: r }));"
        )
        self.assertIsNone(out['result'],
                          "_gdriveInferFormat must return null for Google Sheets MIME")

    def test_infer_format_slides_returns_null(self):
        """_gdriveInferFormat must return null for Google Slides (unsupported)."""
        out = self._run_js(
            "const r = _gdriveInferFormat('application/vnd.google-apps.presentation');\n"
            "console.log(JSON.stringify({ result: r }));"
        )
        self.assertIsNone(out['result'],
                          "_gdriveInferFormat must return null for Google Slides MIME")

    def test_infer_format_unknown_mime_returns_null(self):
        """_gdriveInferFormat must return null for unknown MIME types."""
        out = self._run_js(
            "const r = _gdriveInferFormat('application/octet-stream');\n"
            "console.log(JSON.stringify({ result: r }));"
        )
        self.assertIsNone(out['result'])

    # ── Unsupported workspace membership ────────────────────────────────────

    def test_unsupported_workspace_has_sheets(self):
        """_GDRIVE_UNSUPPORTED_WORKSPACE.has() must return true for Sheets."""
        out = self._run_js(
            "console.log(JSON.stringify({"
            " sheets: _GDRIVE_UNSUPPORTED_WORKSPACE.has('application/vnd.google-apps.spreadsheet')"
            "}));"
        )
        self.assertTrue(out['sheets'])

    def test_unsupported_workspace_has_slides(self):
        """_GDRIVE_UNSUPPORTED_WORKSPACE.has() must return true for Slides."""
        out = self._run_js(
            "console.log(JSON.stringify({"
            " slides: _GDRIVE_UNSUPPORTED_WORKSPACE.has('application/vnd.google-apps.presentation')"
            "}));"
        )
        self.assertTrue(out['slides'])

    def test_unsupported_workspace_excludes_pdf(self):
        """_GDRIVE_UNSUPPORTED_WORKSPACE.has() must return false for PDF."""
        out = self._run_js(
            "console.log(JSON.stringify({"
            " pdf: _GDRIVE_UNSUPPORTED_WORKSPACE.has('application/pdf')"
            "}));"
        )
        self.assertFalse(out['pdf'])

    # ── Picker callback: Sheets rejection ───────────────────────────────────

    def test_picker_callback_rejects_sheets(self):
        """_gdrivePickerCallback with Sheets MIME must set error message and leave _gdriveSelectedFile null."""
        out = self._run_js(
            "_gdrivePickerCallback({"
            "  action: 'picked',"
            "  docs: [{ id: 'SHEET1', name: 'budget.xlsx', mimeType: 'application/vnd.google-apps.spreadsheet' }]"
            "});\n"
            "console.log(JSON.stringify({"
            "  selectedFile: _gdriveSelectedFile,"
            "  btnDisabled: _elements['gdriveBtnIngest'].disabled,"
            "  msgContainsSheets: _elements['gdriveOut'].textContent.includes('Google Sheets')"
            "}));"
        )
        self.assertIsNone(out['selectedFile'],
                          "_gdriveSelectedFile must remain null after Sheets rejection")
        self.assertTrue(out['btnDisabled'],
                        "Ingest button must remain disabled after Sheets rejection")
        self.assertTrue(out['msgContainsSheets'],
                        "Rejection message must mention 'Google Sheets'")

    def test_picker_callback_rejects_slides(self):
        """_gdrivePickerCallback with Slides MIME must set error message and leave _gdriveSelectedFile null."""
        out = self._run_js(
            "_gdrivePickerCallback({"
            "  action: 'picked',"
            "  docs: [{ id: 'SLIDE1', name: 'deck.pptx', mimeType: 'application/vnd.google-apps.presentation' }]"
            "});\n"
            "console.log(JSON.stringify({"
            "  selectedFile: _gdriveSelectedFile,"
            "  msgContainsSlides: _elements['gdriveOut'].textContent.includes('Google Slides')"
            "}));"
        )
        self.assertIsNone(out['selectedFile'],
                          "_gdriveSelectedFile must remain null after Slides rejection")
        self.assertTrue(out['msgContainsSlides'],
                        "Rejection message must mention 'Google Slides'")

    # ── Picker callback: supported file → field population ──────────────────

    def test_picker_callback_populates_source_uri_for_pdf(self):
        """_gdrivePickerCallback with PDF must set gdriveSourceUri to gdrive://{fileId}."""
        out = self._run_js(
            "_gdrivePickerCallback({"
            "  action: 'picked',"
            "  docs: [{ id: 'FILE123', name: 'report.pdf', mimeType: 'application/pdf' }]"
            "});\n"
            "console.log(JSON.stringify({"
            "  sourceUri: _elements['gdriveSourceUri'].value,"
            "  fileName:  _elements['gdriveFileName'].value,"
            "  format:    _elements['gdriveFormat'].value,"
            "  btnEnabled: !_elements['gdriveBtnIngest'].disabled,"
            "  selectedFileId: _gdriveSelectedFile ? _gdriveSelectedFile.id : null"
            "}));"
        )
        self.assertEqual(out['sourceUri'], 'gdrive://FILE123',
                         "source_uri must be 'gdrive://{fileId}'")
        self.assertEqual(out['fileName'], 'report.pdf')
        self.assertEqual(out['format'], 'pdf')
        self.assertTrue(out['btnEnabled'],
                        "Ingest button must be enabled after valid file selection")
        self.assertEqual(out['selectedFileId'], 'FILE123')

    def test_picker_callback_populates_source_uri_for_google_docs(self):
        """_gdrivePickerCallback with Google Docs MIME must set format=docx and gdrive:// URI."""
        out = self._run_js(
            "_gdrivePickerCallback({"
            "  action: 'picked',"
            "  docs: [{ id: 'DOC456', name: 'spec.gdoc', mimeType: 'application/vnd.google-apps.document' }]"
            "});\n"
            "console.log(JSON.stringify({"
            "  sourceUri: _elements['gdriveSourceUri'].value,"
            "  format: _elements['gdriveFormat'].value,"
            "  exportMime: _gdriveSelectedFile ? _gdriveSelectedFile.exportMime : null"
            "}));"
        )
        self.assertEqual(out['sourceUri'], 'gdrive://DOC456')
        self.assertEqual(out['format'], 'docx')
        self.assertIsNotNone(out['exportMime'],
                             "Google Docs selection must set exportMime for DOCX export path")

    def test_picker_callback_cancel_does_not_change_state(self):
        """_gdrivePickerCallback with action=cancel must not modify _gdriveSelectedFile."""
        out = self._run_js(
            "_gdrivePickerCallback({ action: 'cancel', docs: [] });\n"
            "console.log(JSON.stringify({ selectedFile: _gdriveSelectedFile }));"
        )
        self.assertIsNone(out['selectedFile'],
                          "Cancel action must not populate _gdriveSelectedFile")

    # ── Base64 encoding ─────────────────────────────────────────────────────

    def test_array_buffer_to_base64_known_bytes(self):
        """_arrayBufferToBase64 must encode known bytes to the correct base64 string."""
        # "Hello" → SGVsbG8=
        out = self._run_js(
            "const buf = new Uint8Array([72,101,108,108,111]).buffer;\n"
            "console.log(JSON.stringify({ b64: _arrayBufferToBase64(buf) }));"
        )
        self.assertEqual(out['b64'], 'SGVsbG8=',
                         "_arrayBufferToBase64 must produce correct base64 for [72,101,108,108,111] ('Hello')")

    def test_array_buffer_to_base64_empty(self):
        """_arrayBufferToBase64 of empty buffer must return empty string."""
        out = self._run_js(
            "const buf = new ArrayBuffer(0);\n"
            "console.log(JSON.stringify({ b64: _arrayBufferToBase64(buf) }));"
        )
        self.assertEqual(out['b64'], '')

    # ── Config detection ────────────────────────────────────────────────────

    def test_gdrive_configured_true_when_keys_set(self):
        """_gdriveConfigured() must return true when _GDRIVE_CFG has apiKey and clientId."""
        # _configured_html() injects real-looking values
        out = self._run_js(
            "console.log(JSON.stringify({ configured: _gdriveConfigured() }));"
        )
        self.assertTrue(out['configured'],
                        "_gdriveConfigured() must return true when env vars are set")

    def test_gdrive_configured_false_when_keys_empty(self):
        """_gdriveConfigured() must return false when _GDRIVE_CFG.apiKey is empty."""
        # Use unconfigured HTML which injects apiKey:""
        html = _unconfigured_html()
        start = html.find('<script>')
        end = html.rfind('</script>')
        js_body = html[start + len('<script>'):end]
        full_script = _NODE_STUBS + js_body + "\nconsole.log(JSON.stringify({ configured: _gdriveConfigured() }));"
        result = subprocess.run(
            [self._node, '-e', full_script],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            self.fail(f"Node.js execution failed:\n{result.stderr}")
        out = json.loads(result.stdout)
        self.assertFalse(out['configured'],
                         "_gdriveConfigured() must return false when GOOGLE_PICKER_API_KEY is empty")


if __name__ == "__main__":
    unittest.main()
