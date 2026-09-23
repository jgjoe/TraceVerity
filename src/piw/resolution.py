"""One shared dataset resolution and readiness layer for every read surface.

The Web product, the Agent tool boundary, and the MCP server speak different
protocols over the same local datasets, so they must not resolve a `log_id`
differently. `DatasetResolver` owns that single resolution: the built-in BPIC12
baseline resolves from the canonical local database, every other `log_id`
resolves through the local `DatasetRegistry`, and readiness is proven by real
Core reads instead of file existence.

Nothing here computes a process fact, and no resolved dataset publishes an
absolute local machine path.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .core_surface import (
    STATUS_READY,
    STATUS_REGISTERED,
    STATUS_UNAVAILABLE,
    database_health,
)
from .datasets import (
    DEFAULT_REGISTRY_ROOT,
    DatasetDescriptor,
    DatasetRegistry,
    DatasetRegistryError,
    dataset_identity,
)
from .profiles import BPIC2012


@dataclass(frozen=True, slots=True)
class ResolvedDataset:
    """One dataset as a read surface publishes it.

    Only public/local product metadata is exposed: never an absolute local
    machine path, and never a database that has not proven it can serve the
    canonical Core read path.
    """

    log_id: str
    display_name: str
    source_format: str
    database_path: Path
    built_in: bool
    status: str
    dataset_id: str | None = None
    filename: str | None = None
    sha256: str | None = None
    size_bytes: int | None = None

    @property
    def is_ready(self) -> bool:
        return self.status == STATUS_READY

    def as_item(self) -> dict[str, Any]:
        return {
            "built_in": self.built_in,
            "dataset_id": self.dataset_id,
            "display_name": self.display_name,
            "log_id": self.log_id,
            "source_fingerprint": (
                None
                if self.sha256 is None
                else {
                    "filename": self.filename,
                    "sha256": self.sha256,
                    "size_bytes": self.size_bytes,
                }
            ),
            "source_format": self.source_format,
            "status": self.status,
        }


def _canonical_dataset(database_path: Path) -> ResolvedDataset:
    """Resolve the bundled BPIC12 baseline; it is not a registry entry."""

    health = database_health(database_path)
    dataset_id = None
    if health.sha256 is not None and health.size_bytes is not None:
        dataset_id = dataset_identity(
            source_format=BPIC2012.source_format,
            sha256=health.sha256,
            size_bytes=health.size_bytes,
        )
    return ResolvedDataset(
        built_in=True,
        database_path=Path(database_path),
        dataset_id=dataset_id,
        display_name=BPIC2012.display_name,
        filename=health.filename,
        log_id=BPIC2012.log_id,
        sha256=health.sha256,
        size_bytes=health.size_bytes,
        source_format=BPIC2012.source_format,
        status=health.status,
    )


def _registered_dataset(descriptor: DatasetDescriptor) -> ResolvedDataset:
    """Publish one registered descriptor with its proven readiness.

    A descriptor whose database is missing is `registered`; a database that
    exists but cannot serve the canonical read path, or that no longer carries
    the registered source fingerprint, is `unavailable` — never `ready`.
    """

    if not descriptor.database_path.is_file():
        return ResolvedDataset(
            built_in=False,
            database_path=descriptor.database_path,
            dataset_id=descriptor.dataset_id,
            display_name=descriptor.display_name,
            filename=descriptor.source_path.name,
            log_id=descriptor.log_id,
            sha256=descriptor.sha256,
            size_bytes=descriptor.size_bytes,
            source_format=descriptor.source_format,
            status=STATUS_REGISTERED,
        )
    health = database_health(descriptor.database_path)
    consistent = (
        health.is_ready
        and health.sha256 == descriptor.sha256
        and health.size_bytes == descriptor.size_bytes
    )
    return ResolvedDataset(
        built_in=False,
        database_path=descriptor.database_path,
        dataset_id=descriptor.dataset_id,
        display_name=descriptor.display_name,
        filename=health.filename if consistent else descriptor.source_path.name,
        log_id=descriptor.log_id,
        sha256=health.sha256 if consistent else descriptor.sha256,
        size_bytes=health.size_bytes if consistent else descriptor.size_bytes,
        source_format=descriptor.source_format,
        status=STATUS_READY if consistent else STATUS_UNAVAILABLE,
    )


class DatasetResolver:
    """Resolve any ready local dataset by `log_id`.

    The built-in baseline owns its `log_id`: a registry entry under the same
    identifier would make one identifier resolve to two datasets, so that state
    fails closed (`DatasetRegistryError`) instead of resolving ambiguously.
    """

    def __init__(
        self,
        database_path: Path,
        registry_root: Path = DEFAULT_REGISTRY_ROOT,
    ) -> None:
        self._database_path = Path(database_path)
        self._registry = DatasetRegistry(registry_root)

    @property
    def database_path(self) -> Path:
        return self._database_path

    @property
    def registry(self) -> DatasetRegistry:
        return self._registry

    def _require_unshadowed(self) -> None:
        """A registry entry under the built-in identifier makes it ambiguous."""

        if BPIC2012.log_id in self._registry.log_ids():
            raise DatasetRegistryError(
                f"log_id {BPIC2012.log_id!r} is reserved for the built-in dataset "
                "but is also registered"
            )

    def canonical(self) -> ResolvedDataset:
        return _canonical_dataset(self._database_path)

    def registered(self, descriptor: DatasetDescriptor) -> ResolvedDataset:
        return _registered_dataset(descriptor)

    def items(self) -> list[ResolvedDataset]:
        """The canonical baseline first, then every registered dataset by `log_id`."""

        self._require_unshadowed()
        items = [self.canonical()]
        items.extend(
            self.registered(self._registry.get(log_id))
            for log_id in self._registry.log_ids()
        )
        return items

    def find(self, log_id: str) -> ResolvedDataset | None:
        """Resolve one `log_id`, or `None` when it is not a known local dataset.

        A registry that carries the reserved built-in identifier is invalid
        state that makes one identifier ambiguous, so every resolution fails
        closed until the registry is corrected.
        """

        self._require_unshadowed()
        if log_id == BPIC2012.log_id:
            return self.canonical()
        descriptor = self._registry.find(log_id)
        return None if descriptor is None else self.registered(descriptor)
