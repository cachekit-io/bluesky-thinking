"""LAB-1131 scratch: reproduces the pytest lines Kody's base-image rule
falsely flagged on bluesky-thinking#8 (ingester/tests/test_secure.py:4,7,8,10).

This file contains no FROM, no image reference, no Dockerfile content.
The base-image pinning rule must NOT fire here.
"""

import pytest
from pydantic import SecretStr

from skyline_ingester import NAMESPACE
from skyline_ingester.config import Settings


def test_placeholder_never_runs():
    pytest.skip("LAB-1131 scratch fixture — not a real test")
    assert isinstance(SecretStr("x"), SecretStr)
    assert NAMESPACE
    assert Settings
