"""Smoke test - verifies omnixys-cache can be imported."""

from __future__ import annotations

import importlib



def test_package_importable() -> None:
    mod = importlib.import_module("omnixys_cache")
    assert hasattr(mod, "__version__")
    assert mod.__version__ == "1.0.0"


def test_public_api() -> None:
    from omnixys_cache import client, invalidation, serializer

    assert client is not None
    assert invalidation is not None
    assert serializer is not None
