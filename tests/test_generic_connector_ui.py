"""W-C3.2 Generic Connector Trigger UI tests — knowledge-ingest#46.

Validation approach: two layers.

Layer 1 — HTML/JS string checks: validate that the required DOM elements, JS
functions, and payload fields are present in the rendered /ui response. This is
static analysis of rendered output — not JS execution.

Layer 2 — Node.js execution: extracts the <script> block, adds DOM/API stubs,
and runs via Node.js subprocess. Exercises:
- connOnFileChange: filename auto-populate + base64 strip
- doConnIngest: payload construction, field validation guards
- 413 / 422 / verdict response rendering logic

Honest scope statement:
- The FileReader.readAsDataURL path is tested via Node.js with a synchronous
  mock (FileReader.onload is called synchronously). This validates the base64
  prefix-strip logic and filename population without a real browser FileReader.
- The actual fetch() to /connectors/ingest is not executed in these tests —
  it is mocked. This is intentional: the endpoint is already tested in
  test_connector_ingest_http.py. These tests verify the UI layer's side of
  the contract (payload shape, field validation, response rendering).
- No Google Drive, S3, or Microsoft-specific logic is added or tested here.
  This form is connector-agnostic by design.

Coverage:
  1. Generic connector section renders in /ui.
  2. Required DOM element IDs exist.
  3. connOnFileChange auto-populates filename and sets _connRawB64 [Node.js].
  4. Base64 prefix ('data:...;base64,') is stripped before storing [Node.js].
  5. doConnIngest payload includes connector_module, scope, source_uri, format, raw_bytes_b64.
  6. Submit path is POST /connectors/ingest.
  7. source_revision included in payload only when non-empty [Node.js].
  8. Missing required fields are caught before fetch [Node.js].
  9. 413 response rendered with size limit message.
 10. 422 response rendered with detail text.
 11. SyncResult verdicts (fetched_new, skipped_unchanged, fetch_failed) rendered [Node.js].
 12. No connector-specific (Google, S3, Microsoft) logic added.
 13. No new Python routes or modules added.
"""

import json
import os
import shutil
import subprocess
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from fastapi.testclient import TestClient
import main  # noqa
from main import app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_ui_html() -> str:
    import config as cfg_module
    cfg_module.get_settings.cache_clear()
    try:
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/ui")
        assert resp.status_code == 200, f"/ui returned {resp.status_code}"
        return resp.text
    finally:
        cfg_module.get_settings.cache_clear()


# Node.js stubs — minimal DOM + no-op google/gapi globals so the full script runs.
_NODE_STUBS = """\
const _elements = {
  connOut:      { textContent: '', innerHTML: '', style: {} },
  connModule:   { value: '' },
  connScope:    { value: '' },
  connSourceUri:{ value: '' },
  connFilename: { value: '' },
  connFormat:   { value: 'pdf' },
  connRevision: { value: '' },
  connOwner:    { value: '' },
  connFile:     { files: [] },
  // gdrive stubs (needed because the full script block includes gdrive code)
  gdriveConfigWarning: { style: { display: 'none' } },
  gdriveBtnPick: { disabled: false, style: { opacity: '1' }, title: '' },
  gdriveBtnIngest: { disabled: true, style: { opacity: '0.5' } },
  gdriveOut:    { textContent: '', innerHTML: '', style: {} },
  gdriveFileName: { value: '' }, gdriveSourceUri: { value: '' }, gdriveFormat: { value: '' },
};
const document = {
  getElementById: (id) => _elements[id] || { value: '', style: {}, disabled: false, textContent: '', innerHTML: '' },
  createElement: (tag) => ({ src: '', onload: null, setAttribute: () => {} }),
  head: { appendChild: (el) => { if (el.onload) el.onload(); } },
  readyState: 'complete',
  addEventListener: () => {},
};
const google = {
  picker: {
    Action: { PICKED: 'picked', CANCEL: 'cancel' },
    DocsView: class { setIncludeFolders() { return this; } setSelectFolderEnabled() { return this; } },
    PickerBuilder: class {
      addView() { return this; } setOAuthToken() { return this; }
      setDeveloperKey() { return this; } setCallback() { return this; }
      setAppId() { return this; } build() { return { setVisible: () => {} }; }
    },
    ViewId: { DOCS: 'docs' },
  },
  accounts: { oauth2: { initTokenClient: () => ({ requestAccessToken: () => {} }) } },
};
// Synchronous FileReader stub — calls onload synchronously with a fake data URL.
class FileReader {
  readAsDataURL(file) {
    const b64 = file._b64 || 'SGVsbG8=';
    const mime = file._mime || 'application/pdf';
    if (this.onload) this.onload({ target: { result: 'data:' + mime + ';base64,' + b64 } });
  }
}
// btoa is natively available in Node >= 16.
"""


class _NodeRunner(unittest.TestCase):
    """Mixin providing Node.js JS execution helpers."""

    @classmethod
    def setUpClass(cls):
        cls._node = shutil.which('node')

    def _skip_if_no_node(self):
        if not self._node:
            self.skipTest("Node.js not available")

    def _extract_js(self) -> str:
        html = _get_ui_html()
        start = html.find('<script>')
        end = html.rfind('</script>')
        self.assertGreater(start, -1, "<script> block not found in /ui")
        return html[start + len('<script>'):end]

    def _run_js(self, assertion_code: str) -> dict:
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


# ---------------------------------------------------------------------------
# 1. Section presence
# ---------------------------------------------------------------------------

class TestConnSectionPresent(unittest.TestCase):
    """Generic connector section must exist in /ui."""

    def test_section_heading_exists(self):
        html = _get_ui_html()
        self.assertIn('id="hConn"', html,
                      "hConn heading missing from /ui")

    def test_section_body_exists(self):
        html = _get_ui_html()
        self.assertIn('id="sConn"', html,
                      "sConn section body missing from /ui")

    def test_section_heading_text(self):
        html = _get_ui_html()
        self.assertIn("Generic Connector Ingest", html,
                      "Section heading must contain 'Generic Connector Ingest'")

    def test_references_connectors_ingest_in_prose(self):
        """UI prose must name the submission endpoint."""
        html = _get_ui_html()
        idx = html.find('id="hConn"')
        section = html[idx:idx + 3000]
        self.assertIn("/connectors/ingest", section,
                      "Section prose must reference POST /connectors/ingest")

    def test_explains_when_to_use_vs_gdrive(self):
        """Section must mention Google Drive to distinguish from the picker section."""
        html = _get_ui_html()
        idx = html.find('id="sConn"')
        section = html[idx:idx + 2000]
        self.assertIn("Google Drive", section,
                      "Section must distinguish itself from the Google Drive picker")

    def test_explains_when_to_use_vs_s3(self):
        """Section must mention S3 to distinguish from the S3 sync section."""
        html = _get_ui_html()
        idx = html.find('id="sConn"')
        section = html[idx:idx + 2000]
        self.assertIn("S3", section,
                      "Section must distinguish itself from the S3 sync section")


# ---------------------------------------------------------------------------
# 2. Required DOM element IDs
# ---------------------------------------------------------------------------

class TestConnDOMElements(unittest.TestCase):
    """All ConnectorIngestRequest fields must have corresponding DOM elements."""

    def _section(self) -> str:
        html = _get_ui_html()
        start = html.find('id="hConn"')
        # Read a large slice — the section ends before the GDrive section
        return html[start:start + 5000]

    def test_connector_module_input(self):
        self.assertIn('id="connModule"', self._section())

    def test_scope_input(self):
        self.assertIn('id="connScope"', self._section())

    def test_source_uri_input(self):
        self.assertIn('id="connSourceUri"', self._section())

    def test_filename_input(self):
        self.assertIn('id="connFilename"', self._section())

    def test_format_select(self):
        section = self._section()
        self.assertIn('id="connFormat"', section)

    def test_format_options_complete(self):
        """Format dropdown must include all five supported values."""
        section = self._section()
        for fmt in ('pdf', 'docx', 'html', 'markdown', 'txt'):
            self.assertIn(f'value="{fmt}"', section,
                          f"Format option '{fmt}' missing from connFormat select")

    def test_file_input(self):
        self.assertIn('id="connFile"', self._section())

    def test_file_input_calls_on_change(self):
        """File input must call connOnFileChange on change."""
        section = self._section()
        idx = section.find('id="connFile"')
        surroundings = section[max(0, idx - 20):idx + 200]
        self.assertIn("connOnFileChange", surroundings,
                      "connFile input must call connOnFileChange on change")

    def test_source_revision_input(self):
        self.assertIn('id="connRevision"', self._section())

    def test_owner_input(self):
        self.assertIn('id="connOwner"', self._section())

    def test_output_element(self):
        self.assertIn('id="connOut"', self._section())

    def test_submit_button_calls_do_conn_ingest(self):
        section = self._section()
        self.assertIn("doConnIngest()", section,
                      "Submit button must call doConnIngest()")


# ---------------------------------------------------------------------------
# 3–4. File change handler [Node.js]
# ---------------------------------------------------------------------------

class TestConnFileChange(_NodeRunner):
    """connOnFileChange must auto-populate filename and strip base64 data URL prefix."""

    def test_filename_auto_populated(self):
        """connOnFileChange must set connFilename.value to the file name."""
        out = self._run_js("""
_elements['connFilename'].value = '';
_elements['connOut'].textContent = '';
const fakeFile = { name: 'report.pdf', size: 1234, _b64: 'SGVsbG8=', _mime: 'application/pdf' };
connOnFileChange({ files: [fakeFile] });
console.log(JSON.stringify({ filename: _elements['connFilename'].value }));
""")
        self.assertEqual(out['filename'], 'report.pdf',
                         "connOnFileChange must populate connFilename with the file name")

    def test_base64_prefix_stripped(self):
        """connOnFileChange must strip 'data:<mime>;base64,' prefix before storing."""
        out = self._run_js("""
const fakeFile = { name: 'x.pdf', size: 5, _b64: 'SGVsbG8=', _mime: 'application/pdf' };
connOnFileChange({ files: [fakeFile] });
console.log(JSON.stringify({ b64: _connRawB64 }));
""")
        # Must be pure base64, no 'data:' prefix
        b64 = out['b64']
        self.assertFalse(b64.startswith('data:'),
                         f"_connRawB64 must not contain 'data:' prefix, got: {b64!r}")
        self.assertEqual(b64, 'SGVsbG8=',
                         "Base64 content must match the stub value after prefix strip")

    def test_no_file_leaves_b64_null(self):
        """connOnFileChange with empty files list must not set _connRawB64."""
        out = self._run_js("""
_connRawB64 = null;
connOnFileChange({ files: [] });
console.log(JSON.stringify({ b64: _connRawB64 }));
""")
        self.assertIsNone(out['b64'],
                          "_connRawB64 must remain null when no file is selected")


# ---------------------------------------------------------------------------
# 5–6. Payload construction and submit path
# ---------------------------------------------------------------------------

class TestConnPayload(unittest.TestCase):
    """JS payload must contain all required ConnectorIngestRequest fields."""

    def _fn_body(self) -> str:
        html = _get_ui_html()
        start = html.find("async function doConnIngest()")
        self.assertGreater(start, -1, "doConnIngest not found in /ui JS")
        return html[start:start + 3000]

    def test_connector_module_in_payload(self):
        self.assertIn("connector_module:", self._fn_body())

    def test_scope_in_payload(self):
        self.assertIn("scope,", self._fn_body())

    def test_source_uri_in_payload(self):
        self.assertIn("source_uri:", self._fn_body())

    def test_filename_in_payload(self):
        self.assertIn("filename,", self._fn_body())

    def test_format_in_payload(self):
        self.assertIn("format,", self._fn_body())

    def test_raw_bytes_b64_in_payload(self):
        self.assertIn("raw_bytes_b64:", self._fn_body())

    def test_submit_path_is_connectors_ingest(self):
        self.assertIn("'/connectors/ingest'", self._fn_body())

    def test_method_is_post(self):
        self.assertIn("method: 'POST'", self._fn_body())


# ---------------------------------------------------------------------------
# 7. source_revision conditional inclusion [Node.js]
# ---------------------------------------------------------------------------

class TestConnSourceRevision(_NodeRunner):
    """source_revision must be included in payload only when non-empty."""

    def _mock_fetch_capture(self) -> str:
        """JS setup that captures the payload sent to fetch."""
        return """
let _capturedPayload = null;
const fetch = (url, opts) => {
  _capturedPayload = JSON.parse(opts.body);
  return Promise.resolve({
    ok: true, status: 200,
    json: () => Promise.resolve({ verdict: 'fetched_new', task_id: 'T1' }),
  });
};
"""

    def test_source_revision_included_when_set(self):
        """source_revision must appear in payload when the field has a value."""
        out = self._run_js(
            self._mock_fetch_capture() + """
_connRawB64 = 'SGVsbG8=';
_elements['connModule'].value   = 'test_connector';
_elements['connScope'].value    = 'test/scope';
_elements['connSourceUri'].value= 'test://doc1';
_elements['connFilename'].value = 'doc.pdf';
_elements['connFormat'].value   = 'pdf';
_elements['connRevision'].value = '2024-01-15T10:30:45Z';
_elements['connOwner'].value    = '';
doConnIngest().then(() => {
  console.log(JSON.stringify({ revision: _capturedPayload ? _capturedPayload.source_revision : 'MISSING' }));
});
"""
        )
        self.assertEqual(out['revision'], '2024-01-15T10:30:45Z',
                         "source_revision must be in payload when field is non-empty")

    def test_source_revision_omitted_when_empty(self):
        """source_revision must not appear in payload when the field is blank."""
        out = self._run_js(
            self._mock_fetch_capture() + """
_connRawB64 = 'SGVsbG8=';
_elements['connModule'].value   = 'test_connector';
_elements['connScope'].value    = 'test/scope';
_elements['connSourceUri'].value= 'test://doc1';
_elements['connFilename'].value = 'doc.pdf';
_elements['connFormat'].value   = 'pdf';
_elements['connRevision'].value = '';
_elements['connOwner'].value    = '';
doConnIngest().then(() => {
  const hasRevision = _capturedPayload && 'source_revision' in _capturedPayload;
  console.log(JSON.stringify({ hasRevision }));
});
"""
        )
        self.assertFalse(out['hasRevision'],
                         "source_revision must NOT be in payload when field is empty")


# ---------------------------------------------------------------------------
# 8. Field validation guards [Node.js]
# ---------------------------------------------------------------------------

class TestConnValidation(_NodeRunner):
    """Missing required fields must be caught before fetch is called."""

    def _check_guard(self, field_to_clear: str, expected_msg_substr: str):
        setup = """
let _fetchCalled = false;
const fetch = () => { _fetchCalled = true; return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({}) }); };
_connRawB64 = 'SGVsbG8=';
_elements['connModule'].value    = 'test_mod';
_elements['connScope'].value     = 'test/scope';
_elements['connSourceUri'].value = 'test://x';
_elements['connFilename'].value  = 'file.pdf';
_elements['connFormat'].value    = 'pdf';
_elements['connRevision'].value  = '';
_elements['connOwner'].value     = '';
"""
        clear = f"_elements['{field_to_clear}'].value = '';\n"
        assertion = """
doConnIngest().then(() => {
  console.log(JSON.stringify({
    fetchCalled: _fetchCalled,
    msg: _elements['connOut'].textContent
  }));
});
"""
        out = self._run_js(setup + clear + assertion)
        self.assertFalse(out['fetchCalled'],
                         f"fetch must NOT be called when {field_to_clear} is empty")
        self.assertIn(expected_msg_substr, out['msg'],
                      f"Error message must mention required field when {field_to_clear} is empty")

    def test_missing_connector_module_blocked(self):
        self._check_guard('connModule', 'connector_module')

    def test_missing_scope_blocked(self):
        self._check_guard('connScope', 'Scope')

    def test_missing_source_uri_blocked(self):
        self._check_guard('connSourceUri', 'source_uri')

    def test_missing_filename_blocked(self):
        self._check_guard('connFilename', 'filename')

    def test_no_file_read_blocked(self):
        """doConnIngest must block when _connRawB64 is null (no file read)."""
        out = self._run_js("""
let _fetchCalled = false;
const fetch = () => { _fetchCalled = true; return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({}) }); };
_connRawB64 = null;
_elements['connModule'].value    = 'test_mod';
_elements['connScope'].value     = 'test/scope';
_elements['connSourceUri'].value = 'test://x';
_elements['connFilename'].value  = 'file.pdf';
_elements['connFormat'].value    = 'pdf';
doConnIngest().then(() => {
  console.log(JSON.stringify({ fetchCalled: _fetchCalled, msg: _elements['connOut'].textContent }));
});
""")
        self.assertFalse(out['fetchCalled'],
                         "fetch must not be called when no file has been read")
        self.assertIn('No file', out['msg'])


# ---------------------------------------------------------------------------
# 9–11. Response rendering [Node.js]
# ---------------------------------------------------------------------------

class TestConnResponseRendering(_NodeRunner):
    """413, 422, and SyncResult verdicts must be rendered clearly."""

    def _run_with_mock_resp(self, status: int, body: dict) -> dict:
        mock = f"""
const fetch = () => Promise.resolve({{
  ok: {str(status == 200).lower()},
  status: {status},
  statusText: 'mock',
  json: () => Promise.resolve({json.dumps(body)}),
}});
_connRawB64 = 'SGVsbG8=';
_elements['connModule'].value    = 'test_mod';
_elements['connScope'].value     = 'test/scope';
_elements['connSourceUri'].value = 'test://x';
_elements['connFilename'].value  = 'f.pdf';
_elements['connFormat'].value    = 'pdf';
_elements['connRevision'].value  = '';
_elements['connOwner'].value     = '';
"""
        assertion = """
doConnIngest().then(() => {
  console.log(JSON.stringify({
    text: _elements['connOut'].textContent,
    html: _elements['connOut'].innerHTML,
  }));
});
"""
        return self._run_js(mock + assertion)

    def test_413_rendered_with_limit_message(self):
        out = self._run_with_mock_resp(413, {"detail": "payload too large"})
        text = out['text']
        self.assertIn('413', text, "413 status must appear in output")
        self.assertIn('50 MiB', text, "413 message must state the 50 MiB limit")

    def test_422_rendered_with_detail(self):
        out = self._run_with_mock_resp(422, {"detail": "format must be one of ..."})
        text = out['text']
        self.assertIn('422', text)
        self.assertIn('format must be one of', text,
                      "422 detail must be shown to operator")

    def test_fetched_new_verdict_rendered(self):
        out = self._run_with_mock_resp(200, {
            "verdict": "fetched_new", "task_id": "T1",
            "document_id": "D1", "version": 1,
        })
        combined = (out['text'] or '') + (out['html'] or '')
        self.assertIn('fetched_new', combined)
        self.assertIn('T1', combined)

    def test_skipped_unchanged_verdict_rendered(self):
        out = self._run_with_mock_resp(200, {
            "verdict": "skipped_unchanged", "task_id": "T2",
        })
        combined = (out['text'] or '') + (out['html'] or '')
        self.assertIn('skipped_unchanged', combined)

    def test_fetch_failed_verdict_rendered(self):
        out = self._run_with_mock_resp(200, {
            "verdict": "fetch_failed", "task_id": "T3",
            "error_message": "access denied",
        })
        combined = (out['text'] or '') + (out['html'] or '')
        self.assertIn('fetch_failed', combined)

    def test_error_message_shown_on_fetch_failed(self):
        out = self._run_with_mock_resp(200, {
            "verdict": "fetch_failed", "task_id": "T3",
            "error_message": "access denied",
        })
        combined = (out['text'] or '') + (out['html'] or '')
        self.assertIn('access denied', combined,
                      "error_message from SyncResult must be displayed")


# ---------------------------------------------------------------------------
# 12. No connector-specific logic
# ---------------------------------------------------------------------------

class TestConnAgnostic(unittest.TestCase):
    """The generic section must contain no connector-specific logic."""

    def _section_js(self) -> str:
        html = _get_ui_html()
        # The generic connector JS section is the last section before </script>.
        start = html.find("// ── Generic Connector Trigger (W-C3.2)")
        end = html.find("</script>", start)
        self.assertGreater(start, -1, "Generic connector JS block not found")
        self.assertGreater(end, start, "</script> not found after generic connector JS block")
        return html[start:end]

    def test_no_google_picker_in_generic_section(self):
        js = self._section_js()
        self.assertNotIn("PickerBuilder", js,
                         "Generic connector section must not reference Google PickerBuilder")
        self.assertNotIn("_GDRIVE_CFG", js,
                         "Generic connector section must not reference _GDRIVE_CFG")

    def test_no_oauth_in_generic_section(self):
        js = self._section_js()
        self.assertNotIn("initTokenClient", js,
                         "Generic connector section must not contain OAuth token logic")
        self.assertNotIn("access_token", js,
                         "Generic connector section must not reference OAuth access_token")

    def test_no_s3_specific_fields_in_generic_section(self):
        js = self._section_js()
        self.assertNotIn("s3Bucket", js,
                         "Generic connector section must not reference S3-specific form fields")
        self.assertNotIn("s3SecretRef", js)

    def test_no_microsoft_sdk_in_generic_section(self):
        js = self._section_js()
        self.assertNotIn("msal", js.lower(),
                         "Generic connector section must not reference MSAL/Microsoft SDK")
        self.assertNotIn("OneDrive", js,
                         "Generic connector section must not reference OneDrive")

    def test_connector_module_is_free_text_not_hardcoded(self):
        """connector_module must be read from the form input, not hardcoded."""
        js = self._section_js()
        # Must read from element, not have a hardcoded string like 'google_drive'
        self.assertIn("connModule", js,
                      "connector_module must come from the connModule input element")
        # Must NOT hardcode a connector name in the generic section's payload
        self.assertNotIn("connector_module: 'google_drive'", js,
                         "Generic section must not hardcode connector_module value")
        self.assertNotIn("connector_module: 'aws_s3'", js,
                         "Generic section must not hardcode connector_module value")


# ---------------------------------------------------------------------------
# 13. No new routes or modules
# ---------------------------------------------------------------------------

class TestNoNewRoutesOrModules(unittest.TestCase):

    def test_no_new_generic_connector_route_added(self):
        """connector_routes.py must not have a new route for #46."""
        path = os.path.join(os.path.dirname(__file__), "..", "src", "api", "connector_routes.py")
        with open(path) as f:
            content = f.read()
        # The only endpoint for the generic form is the existing /connectors/ingest
        self.assertNotIn('"/connectors/generic"', content,
                         "No new /connectors/generic route should be added")
        self.assertNotIn('"/connectors/browser"', content)

    def test_no_new_python_module_imported_in_routes(self):
        """routes.py must not import a new module specific to W-C3.2."""
        path = os.path.join(os.path.dirname(__file__), "..", "src", "api", "routes.py")
        with open(path) as f:
            content = f.read()
        self.assertNotIn("import generic_connector", content)
        self.assertNotIn("import browser_connector", content)

    def test_js_functions_defined_in_routes_py(self):
        """connOnFileChange and doConnIngest must be defined inline in routes.py."""
        path = os.path.join(os.path.dirname(__file__), "..", "src", "api", "routes.py")
        with open(path) as f:
            content = f.read()
        self.assertIn("function connOnFileChange", content)
        self.assertIn("async function doConnIngest", content)


if __name__ == "__main__":
    unittest.main()
