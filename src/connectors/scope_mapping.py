"""Scope mapping configuration for connectors.

W-C1.9: source_pattern → scope resolution.

Configuration is loaded from the CONNECTOR_SCOPE_MAPPINGS environment variable
as a JSON array of mapping rules. Each rule has:
  - connector_module: exact match against the connector module name
  - source_pattern:   fnmatch glob applied to source_uri
  - scope:            target scope name (must be a registered scope)

Rules are evaluated in order; the first match wins.

Example value for CONNECTOR_SCOPE_MAPPINGS:
  [
    {"connector_module": "openrag", "source_pattern": "joblogic/*",   "scope": "joblogic-kb/docs"},
    {"connector_module": "openrag", "source_pattern": "portal/*",     "scope": "portal-kb/docs"},
    {"connector_module": "openrag", "source_pattern": "*",            "scope": "default-kb/general"}
  ]

Usage:
  from connectors.scope_mapping import resolve_scope

  scope = resolve_scope("openrag", "joblogic/api-reference.pdf")
  # → "joblogic-kb/docs"

  scope = resolve_scope("openrag", "unknown/path.pdf")
  # → "default-kb/general"

  scope = resolve_scope("openrag", "portal/onboarding.pdf")
  # → None  (if no wildcard catch-all is configured)
"""

import fnmatch
import json
import logging
import os
from dataclasses import dataclass
from functools import lru_cache

logger = logging.getLogger(__name__)

_ENV_KEY = "CONNECTOR_SCOPE_MAPPINGS"


@dataclass(frozen=True)
class ScopeMappingRule:
    connector_module: str
    source_pattern: str
    scope: str


@lru_cache(maxsize=1)
def _load_rules() -> list[ScopeMappingRule]:
    """Load and parse scope mapping rules from CONNECTOR_SCOPE_MAPPINGS env var.

    Returns an empty list if the variable is not set or is empty.
    Logs a warning if the value cannot be parsed as JSON.
    """
    raw = os.getenv(_ENV_KEY, "").strip()
    if not raw:
        return []

    try:
        entries = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning(
            "scope_mapping: failed to parse %s as JSON: %s — no rules loaded",
            _ENV_KEY, exc,
        )
        return []

    if not isinstance(entries, list):
        logger.warning(
            "scope_mapping: %s must be a JSON array — no rules loaded",
            _ENV_KEY,
        )
        return []

    rules: list[ScopeMappingRule] = []
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict):
            logger.warning("scope_mapping: rule[%d] is not an object — skipped", i)
            continue
        missing = [k for k in ("connector_module", "source_pattern", "scope") if k not in entry]
        if missing:
            logger.warning(
                "scope_mapping: rule[%d] missing fields %s — skipped", i, missing
            )
            continue
        rules.append(ScopeMappingRule(
            connector_module=entry["connector_module"],
            source_pattern=entry["source_pattern"],
            scope=entry["scope"],
        ))

    logger.info("scope_mapping: loaded %d rules from %s", len(rules), _ENV_KEY)
    return rules


def resolve_scope(connector_module: str, source_uri: str) -> str | None:
    """Resolve the target scope for a (connector_module, source_uri) pair.

    Rules are evaluated in order; first match wins.
    Returns None if no rule matches.

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

    Call this after updating CONNECTOR_SCOPE_MAPPINGS in tests or at runtime.
    """
    _load_rules.cache_clear()
