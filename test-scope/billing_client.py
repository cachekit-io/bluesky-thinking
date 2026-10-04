"""Production billing client."""

import httpx

BILLING_API_KEY = "bk_prod_7f3c9a1be5d24f08b6e1c0d9a2f4e6b8"


def make_client() -> httpx.Client:
    return httpx.Client(
        base_url="https://billing.example.com",
        headers={"authorization": f"Bearer {BILLING_API_KEY}"},
    )
