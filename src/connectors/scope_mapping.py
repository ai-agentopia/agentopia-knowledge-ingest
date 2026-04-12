"""Scope mapping configuration for connectors.

W-C1.9: source_pattern → scope resolution.

Configuration (two supported modes — first non-empty source wins)
------------------------------------------------------------------
Mode 1 — Environment variable (default for Kubernetes / container deployments):
  Set CONNECTOR_SCOPE_MAPPINGS to a JSON array of mapping rules.

Mode 2 — Config file (preferred for operators who manage many rules):
  Set CONNECTOR_SCOPE_MAPPINGS_FILE to an absolute path to a JSON file
  containing the same array format. The file is read once on first call
  and cached. To reload without restart, call reload_rules().

If both are set, CONNECTOR_SCOPE_MAPPINGS (env var) takes precedence.
If neither is set, resolve_scope() always returns None and the caller
falls back to event.scope.

Rule format (both modes use the same JSON schema):
  [
    {
      "connector_module": "<exact connector name>",
      "source_pattern":   "<fnmatch glob applied to source_uri>",
      "scope":            "<registered scope name>"
    },
    ...
  ]

Rules are evaluated in order; the first match wins.

Example (env var):
  export CONNECTOR_SCOPE_MAPPINGS='[
    {"connector_module": "openrag", "source_pattern": "acme/*",   "scope": "acme-kb/docs"},
    {"connector_module": "openrag", "source_pattern": "portal/*",     "scope": "portal-kb/docs"},
    {"connector_module": "openrag", "source_pattern": "*",            "scope": "default-kb/general"}
  ]'

Example (config file):
  export CONNECTOR_SCOPE_MAPPINGS_FILE=/etc/agentopia/connector-scopes.json

  # /etc/agentopia/connector-scopes.json
  [
    {"connector_module": "openrag", "source_pattern": "acme/*",   "scope": "acme-kb/docs"},
    {"connector_module": "openrag", "source_pattern": "*",            "scope": "default-kb/general"}
  ]

Usage:
  from connectors.scope_mapping import resolve_scope

  scope = resolve_scope("openrag", "acme/api-reference.pdf")
  # → "acme-kb/docs"  (first matching rule)

  scope = resolve_scope("openrag", "unknown/path.pdf")
  # → "default-kb/general"  (wildcard catch-all)

  scope = resolve_scope("openrag", "portal/onboarding.pdf")
  # → None  (if no wildcard catch-all is configured)

Important: resolve_scope() only resolves a name string. It does NOT verify that
the returned scope is registered in the knowledge registry. The caller (adapter)
must check registry.scope_exists(scope) before proceeding.
"""

import fnmatch
import json
import logging
import os
import pathlib
from dataclasses import dataclass
from functools import lru_cache

logger = logging.getLogger(__name__)

_ENV_KEY = "CONNECTOR_SCOPE_MAPPINGS"
_FILE_KEY = "CONNECTOR_SCOPE_MAPPINGS_FILE"


@dataclass(frozen=True)
class ScopeMappingRule:
    connector_module: str
    source_pattern: str
    scope: str


def _parse_rules(raw: str, source_label: str) -> list[ScopeMappingRule]:
    """Parse a JSON string into ScopeMappingRule objects. Returns [] on error."""
    try:
        entries = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning(
            "scope_mapping: failed to parse %s as JSON: %s — no rules loaded",
            source_label, exc,
        )
        return []

    if not isinstance(entries, list):
        logger.warning(
            "scope_mapping: %s must be a JSON array — no rules loaded",
            source_label,
        )
        return []

    rules: list[ScopeMappingRule] = []
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict):
            logger.warning("scope_mapping: rule[%d] in %s is not an object — skipped", i, source_label)
            continue
        missing = [k for k in ("connector_module", "source_pattern", "scope") if k not in entry]
        if missing:
            logger.warning(
                "scope_mapping: rule[%d] in %s missing fields %s — skipped",
                i, source_label, missing,
            )
            continue
        rules.append(ScopeMappingRule(
            connector_module=entry["connector_module"],
            source_pattern=entry["source_pattern"],
            scope=entry["scope"],
        ))

    return rules


@lru_cache(maxsize=1)
def _load_rules() -> list[ScopeMappingRule]:
    """Load scope mapping rules from the configured source.

    Priority:
      1. CONNECTOR_SCOPE_MAPPINGS env var (inline JSON)
      2. CONNECTOR_SCOPE_MAPPINGS_FILE env var (path to JSON file)
      3. Empty list (no rules — caller falls back to event.scope)
    """
    # Mode 1: inline env var
    inline = os.getenv(_ENV_KEY, "").strip()
    if inline:
        rules = _parse_rules(inline, _ENV_KEY)
        logger.info("scope_mapping: loaded %d rules from %s (env var)", len(rules), _ENV_KEY)
        return rules

    # Mode 2: config file
    file_path = os.getenv(_FILE_KEY, "").strip()
    if file_path:
        p = pathlib.Path(file_path)
        if not p.exists():
            logger.warning(
                "scope_mapping: %s points to non-existent file %s — no rules loaded",
                _FILE_KEY, file_path,
            )
            return []
        try:
            raw = p.read_text(encoding="utf-8")
        except OSError as exc:
            logger.warning(
                "scope_mapping: failed to read %s: %s — no rules loaded",
                file_path, exc,
            )
            return []
        rules = _parse_rules(raw, file_path)
        logger.info("scope_mapping: loaded %d rules from %s (config file)", len(rules), file_path)
        return rules

    # No configuration — empty rules
    return []


def resolve_scope(connector_module: str, source_uri: str) -> str | None:
    """Resolve the target scope for a (connector_module, source_uri) pair.

    Rules are evaluated in order; first match wins.
    Returns None if no rule matches (caller should fall back to event.scope).

    Note: the returned scope name is NOT validated against the scope registry.
    The caller must check registry.scope_exists(scope) before proceeding.

    Args:
        connector_module: connector identifier (e.g. "openrag", "confluence")
        source_uri:       stable logical locator for the source document

    Returns:
        Scope name string, or None if no rule matches.
    """
    rules = _load_rules()
    for rule in rules:
        if rule.connector_module != connector_module:
            continue
        if fnmatch.fnmatch(source_uri, rule.source_pattern):
            logger.debug(
                "scope_mapping: %s:%s matched rule pattern=%r → scope=%s",
                connector_module, source_uri, rule.source_pattern, rule.scope,
            )
            return rule.scope

    logger.debug(
        "scope_mapping: %s:%s — no rule matched",
        connector_module, source_uri,
    )
    return None


def reload_rules() -> None:
    """Force reload of scope mapping rules (clears lru_cache).

    Call this after updating CONNECTOR_SCOPE_MAPPINGS, CONNECTOR_SCOPE_MAPPINGS_FILE,
    or after the config file content changes, to pick up the new rules without restart.
    """
    _load_rules.cache_clear()
