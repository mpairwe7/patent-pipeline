"""Ingest stage — populate data/raw/ from either the bundled sample or the
real USPTO PatentsView bulk download.

* Sample mode (default) copies every file under data/sample/ → data/raw/.
* Download mode streams each URL with httpx, showing a tqdm progress bar,
  unzips .tsv.zip archives, and writes the TSV into data/raw/.
"""

from __future__ import annotations

import shutil
import zipfile
from collections.abc import Iterable
from pathlib import Path

import httpx
from tqdm import tqdm

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


def ingest_from_url(urls: Iterable[str], settings: Settings, timeout: float = 60.0) -> list[Path]:
    """Download each URL into data/raw/, unzipping .zip archives."""
    raw_dir = settings.paths.raw_dir
    raw_dir.mkdir(parents=True, exist_ok=True)
    fetched: list[Path] = []

    with httpx.Client(follow_redirects=True, timeout=timeout) as client:
        for url in urls:
            fname = Path(url).name
            dst = raw_dir / fname
            logger.info(f"GET {url}")
            with client.stream("GET", url) as response:
                response.raise_for_status()
                total = int(response.headers.get("Content-Length", 0))
                with (
                    dst.open("wb") as f,
                    tqdm(total=total, unit="B", unit_scale=True, desc=fname, leave=False) as bar,
                ):
                    for chunk in response.iter_bytes(chunk_size=64 * 1024):
                        f.write(chunk)
                        bar.update(len(chunk))
            fetched.append(dst)

            if dst.suffix == ".zip":
                with zipfile.ZipFile(dst) as zf:
                    zf.extractall(raw_dir)
                logger.info(f"unzipped {dst.name} into {raw_dir}")

    return fetched


def ingest(
    settings: Settings, use_sample: bool | None = None, urls: list[str] | None = None
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
    return ingest_from_url(chosen_urls, settings)
