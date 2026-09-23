"""Exact-source verification profiles.

Generic structural validation applies to every dataset. A profile here is a
compatibility gate for one known source and never participates in metrics.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .datasets import (
    SOURCE_FORMAT_XES,
    DatasetDescriptor,
    SourceFingerprint,
    SourceProvenance,
    source_fingerprint,
)


@dataclass(frozen=True, slots=True)
class SourceExpectations:
    """Exact expectations for one canonical source."""

    log_id: str
    display_name: str
    source_format: str
    sha256: str
    size_bytes: int
    case_count: int
    event_count: int
    provenance: SourceProvenance = SourceProvenance()

    def descriptor(self, source_path: Path, database_path: Path) -> DatasetDescriptor:
        """Describe the source that is actually present.

        The pinned `sha256`/`size_bytes` above stay expectations; they are
        verified after ingest so a mismatch is reported instead of hidden.
        """

        source_path = Path(source_path)
        fingerprint = source_fingerprint(source_path, self.provenance)
        return DatasetDescriptor(
            log_id=self.log_id,
            display_name=self.display_name,
            source_format=self.source_format,
            source_path=source_path,
            database_path=Path(database_path),
            sha256=fingerprint.sha256,
            size_bytes=fingerprint.size_bytes,
            provenance=self.provenance,
        )

    def expectations(
        self, raw: dict[str, Any], fingerprint: SourceFingerprint
    ) -> list[tuple[str, bool, Any]]:
        return [
            (
                f"expected_{self.log_id}_sha256",
                fingerprint.sha256 == self.sha256,
                fingerprint.sha256,
            ),
            (
                f"expected_{self.log_id}_file_size",
                fingerprint.size_bytes == self.size_bytes,
                fingerprint.size_bytes,
            ),
            (
                f"expected_{self.log_id}_case_count",
                raw["case_count"] == self.case_count,
                raw["case_count"],
            ),
            (
                f"expected_{self.log_id}_event_count",
                raw["event_count"] == self.event_count,
                raw["event_count"],
            ),
        ]


BPIC2012 = SourceExpectations(
    log_id="bpic2012",
    display_name="BPI Challenge 2012",
    source_format=SOURCE_FORMAT_XES,
    sha256="5cd9cc16b9bcb20bd4aae45666a5d87479ddbf47e6371618b6ad217174cecdf3",
    size_bytes=3_342_406,
    case_count=13_087,
    event_count=262_200,
    provenance=SourceProvenance(
        dataset_url="https://data.4tu.nl/articles/dataset/BPI_Challenge_2012/12689204",
        doi="10.4121/uuid:3926db30-f712-4394-aebc-75976070e91f",
        source_url="https://ndownloader.figshare.com/files/24027287",
    ),
)
