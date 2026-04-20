"""Tests for connectors/managed_relay.py.

Boundary: boto3 S3 client is mocked throughout. No real AWS calls.

Coverage:
  - safe_filename: sanitisation cases (path traversal, special chars, empty)
  - put_file: success (new object), success (replace), S3 error → RuntimeError
  - relay_files: all-success, partial error, all-error
  - delete_file: exists → True, not-exists (S3 raises) → False
"""

import sys
import types
from dataclasses import replace
from unittest.mock import MagicMock, patch, call

import pytest

# ---------------------------------------------------------------------------
# Synthetic boto3 shim so the module can be imported without the real library
# ---------------------------------------------------------------------------

_fake_boto3 = types.ModuleType("boto3")

def _fake_client(service, **kwargs):
    return MagicMock()

_fake_boto3.client = _fake_client
sys.modules.setdefault("boto3", _fake_boto3)

# Now import the module under test
from connectors.managed_relay import (
    ManagedSourceRef,
    RelayError,
    RelayResult,
    delete_file,
    put_file,
    relay_files,
    safe_filename,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _ref(**overrides) -> ManagedSourceRef:
    defaults = dict(
        source_id="00000000-0000-0000-0000-000000000001",
        bucket="agentopia-knowledge-dev",
        prefix="scopes/scope-1/sources/src-1/",
        region="ap-northeast-1",
        access_key="AKIATEST",
        secret_key="testsecret",
        endpoint_url=None,
    )
    defaults.update(overrides)
    return ManagedSourceRef(**defaults)


def _make_s3_mock(etag='"abc123"', head_raises=True):
    """Return a mock S3 client.

    head_raises=True  → HEAD raises (object does not exist → replaced=False)
    head_raises=False → HEAD succeeds (object exists → replaced=True)
    """
    s3 = MagicMock()
    if head_raises:
        s3.head_object.side_effect = Exception("NoSuchKey")
    s3.put_object.return_value = {"ETag": etag}
    return s3


# ---------------------------------------------------------------------------
# safe_filename
# ---------------------------------------------------------------------------

class TestSafeFilename:
    def test_plain_filename_unchanged(self):
        assert safe_filename("report.pdf") == "report.pdf"

    def test_spaces_replaced(self):
        assert safe_filename("my report v2.docx") == "my_report_v2.docx"

    def test_path_traversal_stripped(self):
        assert safe_filename("../../../etc/passwd") == "passwd"

    def test_backslash_path_stripped(self):
        assert safe_filename(r"subdir\document.txt") == "document.txt"

    def test_special_chars_replaced(self):
        # space→_, (→_, v2, )→_, space→_, [→_, final, ]→_, !→_, .pdf
        result = safe_filename("doc (v2) [final]!.pdf")
        assert result == "doc__v2___final__.pdf"

    def test_empty_string_returns_unnamed(self):
        assert safe_filename("") == "_unnamed"

    def test_only_slashes_returns_unnamed(self):
        assert safe_filename("///") == "_unnamed"

    def test_hyphen_and_underscore_kept(self):
        assert safe_filename("my-doc_v2.md") == "my-doc_v2.md"


# ---------------------------------------------------------------------------
# put_file
# ---------------------------------------------------------------------------

class TestPutFile:
    def test_new_object_returns_result_replaced_false(self):
        ref = _ref()
        s3 = _make_s3_mock(etag='"etag001"', head_raises=True)
        with patch("connectors.managed_relay._build_s3_client", return_value=s3):
            result = put_file(ref, "doc.pdf", b"pdfbytes", "application/pdf")

        assert isinstance(result, RelayResult)
        assert result.filename == "doc.pdf"
        assert result.s3_key == "scopes/scope-1/sources/src-1/doc.pdf"
        assert result.bytes_written == 8
        assert result.etag == "etag001"
        assert result.replaced is False

        s3.put_object.assert_called_once_with(
            Bucket="agentopia-knowledge-dev",
            Key="scopes/scope-1/sources/src-1/doc.pdf",
            Body=b"pdfbytes",
            ContentType="application/pdf",
        )

    def test_existing_object_returns_replaced_true(self):
        ref = _ref()
        s3 = _make_s3_mock(etag='"etag002"', head_raises=False)
        with patch("connectors.managed_relay._build_s3_client", return_value=s3):
            result = put_file(ref, "doc.pdf", b"newbytes")

        assert result.replaced is True
        assert result.bytes_written == 8

    def test_filename_is_sanitized_in_key(self):
        ref = _ref()
        s3 = _make_s3_mock()
        with patch("connectors.managed_relay._build_s3_client", return_value=s3):
            result = put_file(ref, "path/traversal/../secret.pdf", b"x")

        assert result.s3_key == "scopes/scope-1/sources/src-1/secret.pdf"

    def test_s3_error_raises_runtime_error(self):
        ref = _ref()
        s3 = MagicMock()
        s3.head_object.side_effect = Exception("NoSuchKey")
        s3.put_object.side_effect = Exception("AccessDenied")
        with patch("connectors.managed_relay._build_s3_client", return_value=s3):
            with pytest.raises(RuntimeError, match="AccessDenied"):
                put_file(ref, "doc.txt", b"bytes")

    def test_etag_stripped_of_quotes(self):
        ref = _ref()
        s3 = _make_s3_mock(etag='"quoted-etag"')
        with patch("connectors.managed_relay._build_s3_client", return_value=s3):
            result = put_file(ref, "file.md", b"content")
        assert result.etag == "quoted-etag"

    def test_endpoint_url_passed_to_client(self):
        ref = _ref(endpoint_url="http://minio:9000")
        captured = {}

        def fake_client_factory(r):
            # Inspect that endpoint_url was passed via ManagedSourceRef
            captured["ref"] = r
            return _make_s3_mock()

        with patch("connectors.managed_relay._build_s3_client", side_effect=fake_client_factory):
            put_file(ref, "f.txt", b"x")

        assert captured["ref"].endpoint_url == "http://minio:9000"


# ---------------------------------------------------------------------------
# relay_files
# ---------------------------------------------------------------------------

class TestRelayFiles:
    def _make_files(self, *names):
        return [(n, f"content-{n}".encode(), "text/plain") for n in names]

    def test_all_success(self):
        ref = _ref()
        files = self._make_files("a.txt", "b.txt")
        s3 = _make_s3_mock()
        with patch("connectors.managed_relay._build_s3_client", return_value=s3):
            results, errors = relay_files(ref, files)

        assert len(results) == 2
        assert len(errors) == 0
        assert {r.filename for r in results} == {"a.txt", "b.txt"}

    def test_partial_error_continues(self):
        ref = _ref()
        files = self._make_files("ok.txt", "bad.txt", "also-ok.txt")

        call_count = [0]

        def fake_put(r, filename, content, content_type="application/octet-stream"):
            call_count[0] += 1
            if filename == "bad.txt":
                raise RuntimeError("simulated S3 failure")
            return RelayResult(
                filename=filename,
                s3_key=ref.prefix + filename,
                bytes_written=len(content),
                etag="etag",
                replaced=False,
            )

        with patch("connectors.managed_relay.put_file", side_effect=fake_put):
            results, errors = relay_files(ref, files)

        assert len(results) == 2
        assert len(errors) == 1
        assert errors[0].filename == "bad.txt"
        assert "simulated S3 failure" in errors[0].error
        assert call_count[0] == 3  # all three files attempted

    def test_empty_batch(self):
        ref = _ref()
        results, errors = relay_files(ref, [])
        assert results == []
        assert errors == []

    def test_all_errors(self):
        ref = _ref()
        files = self._make_files("x.txt", "y.txt")

        def always_fail(r, filename, content, content_type="application/octet-stream"):
            raise RuntimeError("fail")

        with patch("connectors.managed_relay.put_file", side_effect=always_fail):
            results, errors = relay_files(ref, files)

        assert len(results) == 0
        assert len(errors) == 2


# ---------------------------------------------------------------------------
# delete_file
# ---------------------------------------------------------------------------

class TestDeleteFile:
    def test_existing_object_deleted_returns_true(self):
        ref = _ref()
        s3 = MagicMock()
        with patch("connectors.managed_relay._build_s3_client", return_value=s3):
            result = delete_file(ref, "to-delete.pdf")

        assert result is True
        s3.delete_object.assert_called_once_with(
            Bucket="agentopia-knowledge-dev",
            Key="scopes/scope-1/sources/src-1/to-delete.pdf",
        )

    def test_s3_error_returns_false(self):
        ref = _ref()
        s3 = MagicMock()
        s3.delete_object.side_effect = Exception("AccessDenied")
        with patch("connectors.managed_relay._build_s3_client", return_value=s3):
            result = delete_file(ref, "missing.pdf")

        assert result is False

    def test_filename_sanitized_in_delete_key(self):
        ref = _ref()
        s3 = MagicMock()
        with patch("connectors.managed_relay._build_s3_client", return_value=s3):
            delete_file(ref, "../escape.pdf")

        _, kwargs = s3.delete_object.call_args
        assert kwargs["Key"].endswith("escape.pdf")
        assert ".." not in kwargs["Key"]
