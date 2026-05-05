"""Ingest stage — populate ``data/raw/`` from either the bundled sample or
the real USPTO PatentsView bulk download (1976-01-01 → 2025-09-30).

Two modes:

* **Sample** (default) — copies every TSV under ``data/sample/`` into
  ``data/raw/``. No network, fully deterministic, used by tests and CI.

* **Real download** — streaming, resumable, manifest-tracked downloads of
  the PatentsView bulk-data ZIPs. The actual heavy lifting lives in
  :mod:`patent_pipeline.chunked`. Files are streamed in 1 MiB blocks with
  HTTP Range resume; ZIPs are extracted on the fly and the archive is
  removed once unpacked. A ``data/raw/manifest.json`` records every
  finished file (URL · bytes · sha256 · timestamp) so subsequent runs are
  no-ops unless ``--force-refresh`` is passed.
"""

from __future__ import annotations

import shutil
from collections.abc import Iterable
from pathlib import Path

from patent_pipeline.chunked import fetch_with_manifest
from patent_pipeline.config import Settings
from patent_pipeline.logging_setup import logger


def ingest_from_sample(settings: Settings) -> list[Path]:
    """Copy every file in ``data/sample/`` to ``data/raw/``."""
    sample_dir = settings.paths.sample_dir
    raw_dir = settings.paths.raw_dir
    raw_dir.mkdir(parents=True, exist_ok=True)

    sources = sorted(sample_dir.glob("*.tsv"))
    if not sources:
        raise FileNotFoundError(
            f"No sample TSVs found in {sample_dir}. Run `python scripts/make_sample.py` first."
        )

    copied: list[Path] = []
    for src in sources:
        dst = raw_dir / src.name
        shutil.copy2(src, dst)
        copied.append(dst)
        logger.info(f"sample → raw: {src.name} ({dst.stat().st_size:,} bytes)")
    return copied


def ingest_from_url(
    urls: Iterable[str],
    settings: Settings,
    *,
    force: bool = False,
    timeout: float = 120.0,
) -> list[Path]:
    """Stream-download each URL into ``data/raw/`` with chunked I/O,
    Range-resume, and manifest-based deduplication. ZIPs are extracted on
    the fly.
    """
    fetched, manifest = fetch_with_manifest(
        urls, settings.paths.raw_dir, timeout=timeout, force=force
    )
    logger.info(
        f"download complete: {len(fetched)} files materialised, "
        f"{len(manifest.entries)} manifest entries"
    )
    return fetched


def ingest(
    settings: Settings,
    use_sample: bool | None = None,
    urls: list[str] | None = None,
    *,
    force_refresh: bool = False,
) -> list[Path]:
    """High-level entry point used by the CLI."""
    use_sample = settings.ingest.use_sample if use_sample is None else use_sample
    if use_sample:
        logger.info("Ingest mode: sample")
        return ingest_from_sample(settings)

    chosen_urls = urls or list(settings.ingest.sources.values())
    if not chosen_urls:
        raise ValueError("No URLs configured. Provide --url or set ingest.sources.")
    logger.info(f"Ingest mode: download ({len(chosen_urls)} URLs)")
    return ingest_from_url(chosen_urls, settings, force=force_refresh)
