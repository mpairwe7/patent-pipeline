"""Loguru configuration — one place to control log format and level."""

from __future__ import annotations

import sys

from loguru import logger


def configure(level: str = "INFO") -> None:
    """Replace the default handler with a pretty, colour-aware sink."""
    logger.remove()
    logger.add(
        sys.stderr,
        level=level,
        format=(
            "<green>{time:HH:mm:ss}</green> | "
            "<level>{level: <7}</level> | "
            "<cyan>{module}</cyan>:<cyan>{function}</cyan> — "
            "<level>{message}</level>"
        ),
        backtrace=False,
        diagnose=False,
    )


__all__ = ["configure", "logger"]
