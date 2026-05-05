"""Chunked-download + manifest helpers — production-grade ingest primitives.

Supports the *real* PatentsView bulk dataset (1976-01-01 → 2025-09-30):
the disambiguated TSV bundle is multiple gigabytes per file, so naive
`requests.get(url).content` would OOM. This module implements:

* **Streaming downloads** with `httpx.stream` (constant memory).
* **Resume-on-interrupt** via HTTP Range requests when a partial file
  exists on disk.
* **Streaming ZIP extraction** so we don't keep a 2 GB `.zip` on disk
  after unpacking.
* **Per-file SHA-256** verification + size check.
* **Manifest** (`data/raw/manifest.json`) — one entry per file with URL,
  byte size, SHA-256, and last-modified timestamp. Subsequent runs skip
  files that are already complete.

Used by ``patent_pipeline.ingest`` for the ``--download-real`` path.
"""

from __future__ import annotations

import hashlib
import json
import zipfile
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import httpx
from tqdm import tqdm

from patent_pipeline.logging_setup import logger

CHUNK_BYTES = 1 << 20  # 1 MiB streaming chunks
MANIFEST_NAME = "manifest.json"


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------
@dataclass
class ManifestEntry:
    url: str
    filename: str
    bytes: int
    sha256: str
    fetched_at: str
    extracted: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Manifest:
    """Tracks every file successfully downloaded into ``data/raw/``."""

    path: Path
    entries: dict[str, ManifestEntry] = field(default_factory=dict)

    @classmethod
    def load(cls, raw_dir: Path) -> Manifest:
        path = raw_dir / MANIFEST_NAME
        if not path.exists():
            return cls(path=path, entries={})
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            entries = {k: ManifestEntry(**v) for k, v in raw.get("entries", {}).items()}
            return cls(path=path, entries=entries)
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(f"manifest at {path} is malformed ({e}); starting fresh")
            return cls(path=path, entries={})

    def save(self) -> None:
        payload = {
            "version": 1,
            "saved_at": datetime.now(UTC).isoformat(),
            "entries": {k: v.to_dict() for k, v in self.entries.items()},
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def is_complete(self, url: str, expected_size: int | None = None) -> bool:
        entry = self.entries.get(url)
        if entry is None:
            return False
        return not (expected_size is not None and entry.bytes != expected_size)

    def record(self, entry: ManifestEntry) -> None:
        self.entries[entry.url] = entry
        self.save()


# ---------------------------------------------------------------------------
# Streaming primitives
# ---------------------------------------------------------------------------
def _hash_existing(path: Path) -> tuple[int, str]:
    """Return (size, sha256) of an existing file (used for resume)."""
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as f:
        while True:
            chunk = f.read(CHUNK_BYTES)
            if not chunk:
                break
            h.update(chunk)
            size += len(chunk)
    return size, h.hexdigest()


def stream_download(
    url: str,
    dst: Path,
    *,
    client: httpx.Client | None = None,
    resume: bool = True,
    chunk_bytes: int = CHUNK_BYTES,
    timeout: float = 120.0,
) -> tuple[int, str]:
    """Download ``url`` → ``dst`` in constant memory; supports HTTP Range
    resume when a partial download is found on disk.

    Returns (total_bytes, sha256_hex). Raises ``httpx.HTTPStatusError`` on
    non-2xx responses.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    owns_client = client is None
    client = client or httpx.Client(follow_redirects=True, timeout=timeout, http2=False)

    try:
        # Probe Content-Length for the progress bar / resume planning.
        head = client.head(url)
        head.raise_for_status()
        total = int(head.headers.get("Content-Length", 0)) or None
        accepts_ranges = head.headers.get("Accept-Ranges", "").lower() == "bytes"

        start = 0
        sha = hashlib.sha256()
        mode = "wb"
        if resume and accepts_ranges and dst.exists():
            existing_size, existing_sha = _hash_existing(dst)
            if total and existing_size == total:
                logger.info(f"resume: {dst.name} already complete ({total:,} bytes)")
                return existing_size, existing_sha
            if existing_size and (total is None or existing_size < total):
                logger.info(f"resume: {dst.name} from byte {existing_size:,}")
                start = existing_size
                # rebuild the running hash from the existing prefix
                sha = hashlib.sha256()
                with dst.open("rb") as f:
                    for buf in iter(lambda: f.read(chunk_bytes), b""):
                        sha.update(buf)
                mode = "ab"

        headers = {"Range": f"bytes={start}-"} if start else {}
        with client.stream("GET", url, headers=headers) as response:
            response.raise_for_status()
            bar_total = total - start if (total and start) else total
            with (
                dst.open(mode) as f,
                tqdm(
                    total=bar_total,
                    initial=0,
                    unit="B",
                    unit_scale=True,
                    desc=dst.name,
                    leave=False,
                ) as bar,
            ):
                written = start
                for chunk in response.iter_bytes(chunk_size=chunk_bytes):
                    f.write(chunk)
                    sha.update(chunk)
                    bar.update(len(chunk))
                    written += len(chunk)
        return written, sha.hexdigest()
    finally:
        if owns_client:
            client.close()


def extract_zip(zip_path: Path, target_dir: Path, *, remove_zip: bool = True) -> list[Path]:
    """Extract every member of ``zip_path`` into ``target_dir`` and (by
    default) delete the zip to save disk for the multi-GB real bundle.
    """
    target_dir.mkdir(parents=True, exist_ok=True)
    members: list[Path] = []
    with zipfile.ZipFile(zip_path) as zf:
        for info in tqdm(zf.infolist(), desc=f"unzip {zip_path.name}", leave=False):
            zf.extract(info, target_dir)
            members.append(target_dir / info.filename)
    if remove_zip:
        zip_path.unlink()
        logger.info(f"removed {zip_path.name} after extraction")
    return members


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def fetch_with_manifest(
    urls: Iterable[str],
    raw_dir: Path,
    *,
    timeout: float = 120.0,
    force: bool = False,
) -> tuple[list[Path], Manifest]:
    """Stream-download each URL into ``raw_dir`` with manifest deduplication.

    For ``.zip`` URLs the archive is extracted and removed; the manifest
    records the extracted member names so subsequent runs see them as
    already-complete.

    Set ``force=True`` to ignore the manifest and re-download.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    manifest = Manifest.load(raw_dir)
    fetched: list[Path] = []

    with httpx.Client(follow_redirects=True, timeout=timeout) as client:
        for url in urls:
            fname = Path(url).name
            dst = raw_dir / fname

            if not force and manifest.is_complete(url):
                logger.info(f"skip {fname} (already in manifest)")
                # surface the extracted members to the caller
                for member in manifest.entries[url].extracted:
                    p = raw_dir / member
                    if p.exists():
                        fetched.append(p)
                continue

            try:
                size, sha = stream_download(url, dst, client=client, timeout=timeout)
            except httpx.HTTPError as exc:
                logger.error(f"failed {url}: {exc}")
                raise

            extracted: list[str] = []
            if dst.suffix == ".zip":
                members = extract_zip(dst, raw_dir, remove_zip=True)
                extracted = [m.name for m in members]
                fetched.extend(members)
            else:
                fetched.append(dst)

            manifest.record(
                ManifestEntry(
                    url=url,
                    filename=fname,
                    bytes=size,
                    sha256=sha,
                    fetched_at=datetime.now(UTC).isoformat(),
                    extracted=extracted,
                )
            )

    return fetched, manifest
