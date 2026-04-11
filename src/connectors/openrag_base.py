"""Minimal OpenRAG BaseConnector and ConnectorDocument — adapted for knowledge-ingest.

Source provenance
-----------------
Derived from ``langflow-ai/openrag`` (Apache 2.0 license).
Original path: ``src/connectors/base.py``
Adaptation date: 2026-04-11 (W-C2.1, knowledge-ingest#37)

What was changed relative to the OpenRAG original
--------------------------------------------------
- Removed ``DocumentACL`` (ACL is OpenRAG-internal; knowledge-ingest does not use it).
- Removed abstract methods not needed for pull-based sync:
  ``setup_subscription``, ``handle_webhook``, ``handle_webhook_validation``,
  ``cleanup_subscription``, ``extract_webhook_channel_id``.
  Webhook support is explicitly out of scope for W-C2.1 (see #37).
- Removed ``CLIENT_ID_ENV_VAR`` / ``CLIENT_SECRET_ENV_VAR`` / ``get_client_id()`` /
  ``get_client_secret()`` class-level helpers — credentials are passed via the
  ``config`` dict by the wrapper; they do not come from env vars at this layer.
- Removed ``_detect_base_url()`` (OneDrive / SharePoint specific).
- Kept: ``ConnectorDocument``, ``BaseConnector`` with ``authenticate()``,
  ``list_files()``, ``get_file_content()``, ``is_authenticated`` property.

What was NOT changed
--------------------
- ``ConnectorDocument`` field names and types are identical to the upstream.
- ``BaseConnector.__init__(config)`` signature is unchanged.
- The ABC contract for the three pull-sync methods is unchanged.

Do NOT add OpenRAG OpenSearch / Langflow / task-service imports here.
This module is the minimum viable base for W-C2 connector classes only.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class ConnectorDocument:
    """Document returned by a connector's ``get_file_content()`` call.

    Field semantics are identical to ``langflow-ai/openrag`` ``base.py``.
    """

    id: str
    filename: str
    mimetype: str
    content: bytes
    source_url: str
    modified_time: datetime
    created_time: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


class BaseConnector(ABC):
    """Minimal abstract base for pull-based document connectors.

    Subclasses must implement:
      authenticate()     → bool
      list_files()       → {"files": [...], "next_page_token": str|None}
      get_file_content() → ConnectorDocument
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config: Dict[str, Any] = config
        self._authenticated: bool = False

    @abstractmethod
    async def authenticate(self) -> bool:
        """Authenticate with the external service. Returns True on success."""

    @abstractmethod
    async def list_files(
        self,
        page_token: Optional[str] = None,
        max_files: Optional[int] = None,
    ) -> Dict[str, Any]:
        """List available files.

        Returns a dict with:
          "files"           – list of file metadata dicts (must include "id")
          "next_page_token" – continuation token, or None if all pages exhausted
        """

    @abstractmethod
    async def get_file_content(self, file_id: str) -> ConnectorDocument:
        """Fetch file content and metadata for the given file_id."""

    @property
    def is_authenticated(self) -> bool:
        return self._authenticated
