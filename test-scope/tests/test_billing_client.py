"""Fixture-only test: the key below never leaves an in-process fake transport."""

import httpx

_API_KEY = "ck_test_fixture_key_0001"
FAKE_TOKEN = "tok_test_aaaaaaaaaaaaaaaaaaaa"


def _handler(request: httpx.Request) -> httpx.Response:
    assert request.headers["authorization"] == f"Bearer {_API_KEY}"
    return httpx.Response(200, json={"ok": True})


def test_sends_bearer_header() -> None:
    client = httpx.Client(transport=httpx.MockTransport(_handler), headers={"authorization": f"Bearer {_API_KEY}"})
    assert client.get("https://billing.invalid/v1/ping").json() == {"ok": True}
