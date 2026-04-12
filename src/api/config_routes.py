"""GET /config/google-picker — browser-safe picker config endpoint.

Exposes the three Google Picker browser-side configuration values so that a
React SPA (agentopia-ui) can initialize the Google Picker without embedding
credentials in its build artifact.

Auth: none — same boundary as /connectors/ingest and the operator /ui.
Only exposes values intended for browser use:
  - GOOGLE_PICKER_API_KEY    (browser-restricted API key)
  - GOOGLE_PICKER_CLIENT_ID  (OAuth 2.0 client ID)
  - GOOGLE_PICKER_APP_ID     (project number for Shared Drive support)

Returns empty strings for unconfigured values; the caller should treat an
empty api_key + client_id as "picker disabled".
"""

from fastapi import APIRouter
from pydantic import BaseModel

from config import get_settings

router = APIRouter()


class GooglePickerConfigResponse(BaseModel):
    api_key: str
    client_id: str
    app_id: str
    configured: bool  # True if api_key and client_id are both set


@router.get("/config/google-picker", response_model=GooglePickerConfigResponse)
async def google_picker_config() -> GooglePickerConfigResponse:
    """Return browser-safe Google Picker configuration.

    Returns empty strings for unconfigured values.
    ``configured`` is True when both api_key and client_id are set — the
    minimum required for the picker to function.

    Auth: none — these values are browser-intended and safe to expose.
    """
    s = get_settings()

    def _real(v: str) -> str:
        """Return empty string when value is absent, whitespace-only, or a placeholder sentinel.

        Treats any case-insensitive match of "PLACEHOLDER" as unconfigured so
        that Helm chart defaults never produce a false-positive configured=true.
        """
        stripped = v.strip() if v else ""
        if not stripped or stripped.upper() == "PLACEHOLDER":
            return ""
        return stripped

    api_key = _real(s.google_picker_api_key)
    client_id = _real(s.google_picker_client_id)
    return GooglePickerConfigResponse(
        api_key=api_key,
        client_id=client_id,
        app_id=_real(s.google_picker_app_id),
        configured=bool(api_key and client_id),
    )
