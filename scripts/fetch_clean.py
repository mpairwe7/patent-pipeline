"""Download the full-corpus clean dataset from Hugging Face.

The full clean bundle is multi-GB and blows past GitHub LFS quotas, so it's
hosted on a Hugging Face Dataset repo instead. This script fetches the
configured files into data/clean/ and verifies sha256 against
config/clean_manifest.json so a corrupted download fails loudly.

Usage:
    uv run python scripts/fetch_clean.py              # Parquet only (recommended)
    uv run python scripts/fetch_clean.py --format csv # also pull the giant CSVs
    uv run python scripts/fetch_clean.py --check      # verify existing files, no download

No external SDK is required; uses stdlib urllib so it works in CI without
adding huggingface_hub as a dependency.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = REPO_ROOT / "config" / "clean_manifest.json"
CLEAN_DIR = REPO_ROOT / "data" / "clean"


def _sha256_of(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def _download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    with urllib.request.urlopen(url) as resp, tmp.open("wb") as f:
        total = int(resp.headers.get("Content-Length", 0))
        seen = 0
        while True:
            block = resp.read(1 << 20)
            if not block:
                break
            f.write(block)
            seen += len(block)
            if total:
                pct = seen * 100 // total
                print(f"  {dest.name}: {seen / 1e6:>8.1f} / {total / 1e6:.1f} MB ({pct}%)", end="\r")
        print()
    tmp.rename(dest)


def _select(manifest: dict, formats: set[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for rel, meta in manifest["files"].items():
        if rel.startswith("parquet/") and "parquet" in formats:
            out[rel] = meta
        elif rel.startswith("csv/") and "csv" in formats:
            out[rel] = meta
    return out


def _local_path(rel: str) -> Path:
    # csv/clean_patents.csv → data/clean/clean_patents.csv
    # parquet/patents.parquet → data/clean/parquet/patents.parquet
    return CLEAN_DIR / rel.split("/", 1)[1] if rel.startswith("csv/") else CLEAN_DIR / rel


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--format",
        choices=["parquet", "csv", "both"],
        default="parquet",
        help="Which artifact set to fetch (default: parquet — small + fast).",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Skip downloads; just verify existing files against the manifest.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if local file matches manifest.",
    )
    args = parser.parse_args(argv)

    manifest = json.loads(MANIFEST_PATH.read_text())
    base_url = manifest["base_url"].rstrip("/")
    formats = {"parquet", "csv"} if args.format == "both" else {args.format}
    selected = _select(manifest, formats)

    failed: list[str] = []
    for rel, meta in selected.items():
        local = _local_path(rel)
        url = f"{base_url}/{rel}"
        if local.exists() and not args.force:
            actual = _sha256_of(local)
            if actual == meta["sha256"]:
                print(f"✓ {local.relative_to(REPO_ROOT)} ({meta['bytes']:,} bytes)")
                continue
            print(f"✗ {local.relative_to(REPO_ROOT)}: sha256 mismatch, re-downloading")

        if args.check:
            failed.append(rel)
            print(f"✗ {local.relative_to(REPO_ROOT)}: missing or stale")
            continue

        print(f"→ fetching {url}")
        try:
            _download(url, local)
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)
            failed.append(rel)
            continue

        actual = _sha256_of(local)
        if actual != meta["sha256"]:
            print(f"  ERROR: sha256 mismatch for {rel}", file=sys.stderr)
            print(f"    expected {meta['sha256']}", file=sys.stderr)
            print(f"    got      {actual}", file=sys.stderr)
            failed.append(rel)

    if failed:
        print(f"\n{len(failed)} file(s) failed:", file=sys.stderr)
        for rel in failed:
            print(f"  - {rel}", file=sys.stderr)
        return 1

    print("\nclean dataset ready.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
