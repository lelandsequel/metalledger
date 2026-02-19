"""
MetalLedger — SINGLE egress module with allowlist enforcement.

ALL external HTTP calls in the system MUST go through `egress_get()` or
`egress_post()`.  Any domain not on the ALLOWLIST raises EgressViolation.

Allowlist: ["metals-api.com", "api.lbma.org.uk"]
"""

from __future__ import annotations

import urllib.parse
from typing import Any, Dict, Optional

import httpx

from common.config import EGRESS_ALLOWLIST

# Re-export for convenience so callers can `from common.egress import ALLOWLIST`
ALLOWLIST: list[str] = EGRESS_ALLOWLIST


class EgressViolation(Exception):
    """Raised when a caller attempts to reach a domain not on the allowlist."""

    def __init__(self, url: str, domain: str) -> None:
        self.url    = url
        self.domain = domain
        super().__init__(
            f"EgressViolation: domain '{domain}' is not on the allowlist. "
            f"Full URL: {url}"
        )


def _extract_domain(url: str) -> str:
    """Return the netloc (host) component of a URL, stripping 'www.'."""
    parsed = urllib.parse.urlparse(url)
    host   = parsed.netloc or parsed.path          # handle protocol-less URLs
    # Strip port if present
    host = host.split(":")[0]
    # Normalise: remove leading 'www.'
    if host.startswith("www."):
        host = host[4:]
    return host.lower()


def _check_allowlist(url: str) -> None:
    """Raise EgressViolation if the URL's domain is not allowed."""
    domain = _extract_domain(url)
    for allowed in ALLOWLIST:
        if domain == allowed or domain.endswith("." + allowed):
            return
    raise EgressViolation(url, domain)


async def egress_get(
    url: str,
    *,
    params:  Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 15.0,
) -> httpx.Response:
    """
    Perform an allowlist-checked HTTP GET.

    Raises:
        EgressViolation: if domain is not on ALLOWLIST
        httpx.HTTPError: on network / HTTP errors
    """
    _check_allowlist(url)
    async with httpx.AsyncClient(timeout=timeout) as client:
        return await client.get(url, params=params, headers=headers)


async def egress_post(
    url: str,
    *,
    json:    Optional[Dict[str, Any]] = None,
    data:    Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 15.0,
) -> httpx.Response:
    """
    Perform an allowlist-checked HTTP POST.

    Raises:
        EgressViolation: if domain is not on ALLOWLIST
        httpx.HTTPError: on network / HTTP errors
    """
    _check_allowlist(url)
    async with httpx.AsyncClient(timeout=timeout) as client:
        return await client.post(url, json=json, data=data, headers=headers)
