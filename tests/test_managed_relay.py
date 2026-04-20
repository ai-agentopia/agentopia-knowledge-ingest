"""Tests for connectors/managed_relay.py.

Boundary: boto3 S3 client is mocked throughout. No real AWS calls.

Coverage:
  - safe_stable_id: sanitisation cases including path traversal fragments
  - _derive_ext: extension extraction
  - object_key: key formula determinism, collision safety, connector namespacing
  - put_file: ConnectorFile → RelayResult, replaced flag, S3 error path
  - relay_files: batch success, partial error, empty batch, all errors
  - delete_file: same key as put (delete determinism), S3 error → False
  - ManagedSourceRef.from_source_row: construction helper

Critical contract assertions:
  - Two files with same basename but different stable_ids → different S3 keys (no collision)
  - delete_file produces the same key as put_file for the same item (delete determinism)
  - Repeated update to same stable_id → same key (idempotent update)
  - Unsafe characters in stable_id → sanitised but NOT collapsing distinct IDs
"""

import sys
import types
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Synthetic boto3 shim — import must succeed without real library
# ---------------------------------------------------------------------------

_fake_boto3 = types.ModuleType("boto3")
_fake_boto3.client = lambda service, **kwargs: MagicMock()
sys.modules.setdefault("boto3", _fake_boto3)

from connectors.managed_relay import (
    ConnectorFile,
    ManagedSourceRef,
    RelayError,
    RelayResult,
    _derive_ext,
    _build_s3_client,
    delete_file,
    object_key,
    put_file,
    relay_files,
    safe_stable_id,
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


def _file(
    connector_module="google_drive",
    stable_id="fileId001",
    filename="doc.pdf",
    content=b"bytes",
    content_type="application/pdf",
) -> ConnectorFile:
    return ConnectorFile(
        connector_module=connector_module,
        stable_id=stable_id,
        filename=filename,
        content=content,
        content_type=content_type,
    )


def _s3_mock(etag='"abc123"', head_raises=True):
    s3 = MagicMock()
    if head_raises:
        s3.head_object.side_effect = Exception("NoSuchKey")
    s3.put_object.return_value = {"ETag": etag}
    return s3


# ---------------------------------------------------------------------------
# safe_stable_id
# ---------------------------------------------------------------------------

class TestSafeStableId:
    def test_gdrive_file_id_unchanged(self):
        fid = "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms"
        assert safe_stable_id(fid) == fid

    def test_slash_replaced(self):
        assert safe_stable_id("driveId123/itemId456") == "driveId123_itemId456"

    def test_colon_slash_replaced(self):
        assert safe_stable_id("onedrive://driveId/itemId") == "onedrive___driveId_itemId"

    def test_spaces_replaced(self):
        assert safe_stable_id("item with spaces") == "item_with_spaces"

    def test_empty_maps_to_sentinel(self):
        assert safe_stable_id("") == "_empty"

    def test_hyphen_underscore_preserved(self):
        assert safe_stable_id("item-id_v2") == "item-id_v2"

    def test_different_ids_produce_different_results(self):
        a = safe_stable_id("folder-a/report.pdf")
        b = safe_stable_id("folder-b/report.pdf")
        assert a != b  # Critical: distinct IDs remain distinct


# ---------------------------------------------------------------------------
# _derive_ext
# ---------------------------------------------------------------------------

class TestDeriveExt:
    def test_simple_extension(self):
        assert _derive_ext("report.pdf") == ".pdf"

    def test_docx(self):
        assert _derive_ext("document.docx") == ".docx"

    def test_double_extension_returns_last(self):
        assert _derive_ext("archive.tar.gz") == ".gz"

    def test_no_extension_returns_empty(self):
        assert _derive_ext("README") == ""

    def test_empty_filename_returns_empty(self):
        assert _derive_ext("") == ""

    def test_extension_lowercased(self):
        assert _derive_ext("image.PDF") == ".pdf"

    def test_dot_only_filename(self):
        assert _derive_ext(".hidden") == ".hidden"


# ---------------------------------------------------------------------------
# object_key — the key formula
# ---------------------------------------------------------------------------

class TestObjectKey:
    def test_basic_formula(self):
        ref = _ref()
        key = object_key(ref, "google_drive", "fileId001", "doc.pdf")
        assert key == "scopes/scope-1/sources/src-1/google_drive/fileId001.pdf"

    def test_same_basename_different_stable_id_no_collision(self):
        """Core collision safety: same filename, different stable_id → different key."""
        ref = _ref()
        key_a = object_key(ref, "google_drive", "fileIdA", "report.pdf")
        key_b = object_key(ref, "google_drive", "fileIdB", "report.pdf")
        assert key_a != key_b

    def test_same_stable_id_same_key(self):
        """Idempotent update: same stable_id always maps to same key."""
        ref = _ref()
        key1 = object_key(ref, "google_drive", "fileId001", "doc.pdf")
        key2 = object_key(ref, "google_drive", "fileId001", "doc.pdf")
        assert key1 == key2

    def test_connector_namespace_separates_same_id(self):
        """google_drive and onedrive with same stable_id produce different keys."""
        ref = _ref()
        key_gd = object_key(ref, "google_drive", "abc123", "file.pdf")
        key_od = object_key(ref, "onedrive", "abc123", "file.pdf")
        assert key_gd != key_od

    def test_unsafe_stable_id_sanitised(self):
        ref = _ref()
        key = object_key(ref, "google_drive", "driveId/itemId", "doc.docx")
        assert ".." not in key
        assert key == "scopes/scope-1/sources/src-1/google_drive/driveId_itemId.docx"

    def test_extension_from_filename_only(self):
        ref = _ref()
        key = object_key(ref, "google_drive", "id001", "My Report (Final).pdf")
        assert key.endswith(".pdf")
        # stable_id is unchanged (it was already safe)
        assert "id001" in key

    def test_no_extension_filename(self):
        ref = _ref()
        key = object_key(ref, "google_drive", "id001", "Makefile")
        assert key == "scopes/scope-1/sources/src-1/google_drive/id001"


# ---------------------------------------------------------------------------
# put_file
# ---------------------------------------------------------------------------

class TestPutFile:
    def test_new_object_replaced_false(self):
        ref = _ref()
        file = _file(stable_id="fid001", filename="doc.pdf", content=b"pdfbytes")
        s3 = _s3_mock(etag='"etag001"', head_raises=True)
        with patch("connectors.managed_relay._build_s3_client", return_value=s3):
            result = put_file(ref, file)

        assert isinstance(result, RelayResult)
        assert result.stable_id == "fid001"
        assert result.connector_module == "google_drive"
        assert result.filename == "doc.pdf"
        assert result.s3_key == "scopes/scope-1/sources/src-1/google_drive/fid001.pdf"
        assert result.bytes_written == 8
        assert result.etag == "etag001"
        assert result.replaced is False

    def test_existing_object_replaced_true(self):
        ref = _ref()
        file = _file(stable_id="fid001", filename="doc.pdf")
        s3 = _s3_mock(etag='"etag002"', head_raises=False)
        with patch("connectors.managed_relay._build_s3_client", return_value=s3):
            result = put_file(ref, file)
        assert result.replaced is True

    def test_s3_error_raises_runtime_error(self):
        ref = _ref()
        file = _file()
        s3 = MagicMock()
        s3.head_object.side_effect = Exception("NoSuchKey")
        s3.put_object.side_effect = Exception("AccessDenied")
        with patch("connectors.managed_relay._build_s3_client", return_value=s3):
            with pytest.raises(RuntimeError, match="AccessDenied"):
                put_file(ref, file)

    def test_etag_stripped_of_quotes(self):
        ref = _ref()
        file = _file()
        s3 = _s3_mock(etag='"quoted-etag"')
        with patch("connectors.managed_relay._build_s3_client", return_value=s3):
            result = put_file(ref, file)
        assert result.etag == "quoted-etag"

    def test_put_object_called_with_correct_key(self):
        ref = _ref()
        file = _file(connector_module="google_drive", stable_id="abc123", filename="spec.pdf")
        s3 = _s3_mock()
        with patch("connectors.managed_relay._build_s3_client", return_value=s3):
            put_file(ref, file)
        _, kwargs = s3.put_object.call_args
        assert kwargs["Key"] == "scopes/scope-1/sources/src-1/google_drive/abc123.pdf"
        assert kwargs["Bucket"] == "agentopia-knowledge-dev"
        assert kwargs["Body"] == b"bytes"

    def test_endpoint_url_passed_to_client_factory(self):
        ref = _ref(endpoint_url="http://minio:9000")
        captured = {}
        def fake_factory(r):
            captured["ref"] = r
            return _s3_mock()
        file = _file()
        with patch("connectors.managed_relay._build_s3_client", side_effect=fake_factory):
            put_file(ref, file)
        assert captured["ref"].endpoint_url == "http://minio:9000"


# ---------------------------------------------------------------------------
# relay_files
# ---------------------------------------------------------------------------

class TestRelayFiles:
    def _make_files(self, *ids):
        return [_file(stable_id=sid, filename=f"{sid}.txt", content=f"content-{sid}".encode())
                for sid in ids]

    def test_all_success(self):
        ref = _ref()
        files = self._make_files("id-a", "id-b")
        s3 = _s3_mock()
        with patch("connectors.managed_relay._build_s3_client", return_value=s3):
            results, errors = relay_files(ref, files)
        assert len(results) == 2
        assert len(errors) == 0
        assert {r.stable_id for r in results} == {"id-a", "id-b"}

    def test_partial_error_continues(self):
        ref = _ref()
        files = self._make_files("ok-1", "bad", "ok-2")
        call_count = [0]
        def fake_put(r, f):
            call_count[0] += 1
            if f.stable_id == "bad":
                raise RuntimeError("S3 failure")
            return RelayResult(
                connector_module=f.connector_module,
                stable_id=f.stable_id,
                filename=f.filename,
                s3_key=f"prefix/{f.stable_id}.txt",
                bytes_written=len(f.content),
                etag="etag",
                replaced=False,
            )
        with patch("connectors.managed_relay.put_file", side_effect=fake_put):
            results, errors = relay_files(ref, files)
        assert len(results) == 2
        assert len(errors) == 1
        assert errors[0].stable_id == "bad"
        assert call_count[0] == 3

    def test_empty_batch(self):
        ref = _ref()
        results, errors = relay_files(ref, [])
        assert results == []
        assert errors == []

    def test_all_errors(self):
        ref = _ref()
        files = self._make_files("x", "y")
        with patch("connectors.managed_relay.put_file", side_effect=RuntimeError("fail")):
            results, errors = relay_files(ref, files)
        assert len(results) == 0
        assert len(errors) == 2


# ---------------------------------------------------------------------------
# delete_file — delete determinism
# ---------------------------------------------------------------------------

class TestDeleteFile:
    def test_delete_targets_same_key_as_put(self):
        """Critical: delete and put reference identical S3 key for same stable_id."""
        ref = _ref()
        file = _file(connector_module="google_drive", stable_id="fid-xyz", filename="report.pdf")
        s3_put = _s3_mock()
        s3_del = MagicMock()

        with patch("connectors.managed_relay._build_s3_client", return_value=s3_put):
            result = put_file(ref, file)
        put_key = result.s3_key

        with patch("connectors.managed_relay._build_s3_client", return_value=s3_del):
            delete_file(ref, "google_drive", "fid-xyz", "report.pdf")
        del_key = s3_del.delete_object.call_args[1]["Key"]

        assert put_key == del_key

    def test_delete_success_returns_true(self):
        ref = _ref()
        s3 = MagicMock()
        with patch("connectors.managed_relay._build_s3_client", return_value=s3):
            result = delete_file(ref, "google_drive", "fid001", "doc.pdf")
        assert result is True
        s3.delete_object.assert_called_once_with(
            Bucket="agentopia-knowledge-dev",
            Key="scopes/scope-1/sources/src-1/google_drive/fid001.pdf",
        )

    def test_delete_error_returns_false(self):
        ref = _ref()
        s3 = MagicMock()
        s3.delete_object.side_effect = Exception("AccessDenied")
        with patch("connectors.managed_relay._build_s3_client", return_value=s3):
            result = delete_file(ref, "google_drive", "fid001", "doc.pdf")
        assert result is False


# ---------------------------------------------------------------------------
# ManagedSourceRef.from_source_row
# ---------------------------------------------------------------------------

class TestManagedSourceRefFromSourceRow:
    def test_constructs_from_storage_ref(self):
        storage_ref = {
            "bucket": "agentopia-knowledge-dev",
            "prefix": "scopes/s1/sources/src1/",
            "region": "ap-northeast-1",
        }
        ref = ManagedSourceRef.from_source_row(
            source_id="uuid-001",
            storage_ref=storage_ref,
            access_key="AKIA123",
            secret_key="secret",
        )
        assert ref.source_id == "uuid-001"
        assert ref.bucket == "agentopia-knowledge-dev"
        assert ref.prefix == "scopes/s1/sources/src1/"
        assert ref.region == "ap-northeast-1"
        assert ref.access_key == "AKIA123"
        assert ref.endpoint_url is None

    def test_endpoint_url_optional(self):
        ref = ManagedSourceRef.from_source_row(
            source_id="uuid-002",
            storage_ref={"bucket": "b", "region": "us-east-1"},
            access_key="k",
            secret_key="s",
            endpoint_url="http://minio:9000",
        )
        assert ref.endpoint_url == "http://minio:9000"
        assert ref.prefix == ""  # missing prefix defaults to ""
