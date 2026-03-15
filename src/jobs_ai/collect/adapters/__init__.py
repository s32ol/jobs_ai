from __future__ import annotations

from collections.abc import Mapping

from ..models import SourceInput
from .ashby import AshbyAdapter
from .base import CollectionAdapter
from .generic import GenericAdapter
from .greenhouse import GreenhouseAdapter
from .lever import LeverAdapter

DEFAULT_ADAPTERS: Mapping[str, CollectionAdapter] = {
    "greenhouse": GreenhouseAdapter(),
    "lever": LeverAdapter(),
    "ashby": AshbyAdapter(),
}
GENERIC_ADAPTER: CollectionAdapter = GenericAdapter()


def select_adapter(
    source: SourceInput,
    *,
    registry: Mapping[str, CollectionAdapter] | None = None,
    generic_adapter: CollectionAdapter | None = None,
) -> CollectionAdapter:
    adapter_registry = DEFAULT_ADAPTERS if registry is None else registry
    default_adapter = GENERIC_ADAPTER if generic_adapter is None else generic_adapter
    if source.portal_type is None:
        return default_adapter
    return adapter_registry.get(source.portal_type, default_adapter)


__all__ = [
    "CollectionAdapter",
    "DEFAULT_ADAPTERS",
    "GENERIC_ADAPTER",
    "select_adapter",
]
