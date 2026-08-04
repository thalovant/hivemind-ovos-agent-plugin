"""Low-overhead latency histograms for the OVOS agent bridge."""

from __future__ import annotations

import logging
from threading import Lock
from typing import Dict, Iterable


DEFAULT_BUCKETS_MS = (
    1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0,
    1_000.0, 2_500.0, 5_000.0, 10_000.0, 30_000.0,
)


class LatencyHistogram:
    """Thread-safe cumulative histogram with fixed-cardinality buckets."""

    def __init__(self, name: str, *, buckets_ms: Iterable[float] = DEFAULT_BUCKETS_MS,
                 log_every: int = 100) -> None:
        self.name = name
        self._bounds = tuple(sorted(float(value) for value in buckets_ms))
        self._buckets = [0] * len(self._bounds)
        self._count = 0
        self._sum_ms = 0.0
        self._log_every = max(0, int(log_every))
        self._lock = Lock()
        self._logger = logging.getLogger(__name__)

    def observe_ms(self, elapsed_ms: float) -> None:
        """Record one non-negative latency observation in milliseconds."""
        value = max(0.0, float(elapsed_ms))
        with self._lock:
            self._count += 1
            self._sum_ms += value
            for index, bound in enumerate(self._bounds):
                if value <= bound:
                    self._buckets[index] += 1
            should_log = bool(
                self._log_every and self._count % self._log_every == 0
            )
        if should_log:
            snapshot = self.snapshot()
            self._logger.info(
                "latency_histogram name=%s count=%d sum_ms=%.3f buckets=%s",
                self.name,
                snapshot["count"],
                snapshot["sum_ms"],
                snapshot["buckets"],
            )

    def snapshot(self) -> Dict[str, object]:
        """Return an immutable, JSON-friendly cumulative snapshot."""
        with self._lock:
            buckets = {
                f"le_{bound:g}": count
                for bound, count in zip(self._bounds, self._buckets)
            }
            buckets["inf"] = self._count
            return {
                "name": self.name,
                "count": self._count,
                "sum_ms": self._sum_ms,
                "buckets": buckets,
            }


BUS_WRITE_QUEUE = LatencyHistogram("hivemind_bus_write_queue_ms")
BUS_WRITE = LatencyHistogram("hivemind_bus_write_ms")
SKILL_HANDLER = LatencyHistogram("hivemind_skill_handler_ms")


def performance_histograms() -> Dict[str, Dict[str, object]]:
    """Return all OVOS-agent performance histograms."""
    return {
        histogram.name: histogram.snapshot()
        for histogram in (BUS_WRITE_QUEUE, BUS_WRITE, SKILL_HANDLER)
    }
