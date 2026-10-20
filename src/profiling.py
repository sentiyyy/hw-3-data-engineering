"""Repeatable time and memory profiling using wall-clock time, RSS and tracemalloc."""
from __future__ import annotations

import threading
import time
import tracemalloc
from typing import Callable, Any

import psutil


def profile_call(func: Callable[[], Any], attempt: int, method: str, chunk_size: int | None = None) -> tuple[Any, dict]:
    process = psutil.Process()
    start_rss = process.memory_info().rss
    peak_rss = [start_rss]
    stop_event = threading.Event()

    def sample_memory() -> None:
        while not stop_event.wait(0.01):
            try:
                peak_rss[0] = max(peak_rss[0], process.memory_info().rss)
            except psutil.Error:
                pass

    was_tracing = tracemalloc.is_tracing()
    if not was_tracing:
        tracemalloc.start()
    tracemalloc.reset_peak()
    sampler = threading.Thread(target=sample_memory, name="rss-profiler", daemon=True)
    sampler.start()
    started = time.perf_counter()
    try:
        result = func()
    finally:
        elapsed = time.perf_counter() - started
        try:
            peak_rss[0] = max(peak_rss[0], process.memory_info().rss)
        except psutil.Error:
            pass
        stop_event.set()
        sampler.join(timeout=1)
        _, traced_peak = tracemalloc.get_traced_memory()
        if not was_tracing:
            tracemalloc.stop()

    metrics = {
        "method": method,
        "chunk_size": chunk_size,
        "attempt": attempt,
        "elapsed_seconds": round(elapsed, 6),
        "rss_start_mb": round(start_rss / (1024 * 1024), 3),
        "rss_peak_mb": round(peak_rss[0] / (1024 * 1024), 3),
        "rss_peak_delta_mb": round(max(0, peak_rss[0] - start_rss) / (1024 * 1024), 3),
        "tracemalloc_peak_mb": round(traced_peak / (1024 * 1024), 3),
    }
    return result, metrics
