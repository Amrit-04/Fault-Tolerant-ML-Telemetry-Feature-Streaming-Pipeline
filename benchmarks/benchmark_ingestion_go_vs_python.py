"""
Comparative Ingestion Benchmark: Go Ingestor vs Python Ingestor.

Blasts 50,000 requests under 50 concurrent workers against the Python
FastAPI ingestor vs the Go telemetry ingestor.

Measures:
- Sustained throughput (Requests Per Second)
- p50, p95, p99 latency (ms)
- CPU utilization (%) and Memory footprint (MB) during peak load

Saves structured results to benchmarks/go_ingestion_results.json.
"""

import argparse
import asyncio
import json
import logging
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
import math
import statistics
from typing import Any

import httpx
import psutil

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger("ingestion_benchmark")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESULTS_FILE = PROJECT_ROOT / "benchmarks" / "go_ingestion_results.json"


def calc_percentile(sorted_data: list[float], pct: float) -> float:
    """Calculate percentile from a sorted list."""
    if not sorted_data:
        return 0.0
    k = (len(sorted_data) - 1) * (pct / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_data[int(k)]
    return sorted_data[int(f)] * (c - k) + sorted_data[int(c)] * (k - f)


def generate_payload(seq: int) -> dict[str, Any]:
    """Generate a valid schema-compliant ML telemetry event."""
    return {
        "event_id": str(uuid.uuid4()),
        "entity_id": f"device-{seq % 1000:04d}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "feature_vector": [
            round(float(x) * 0.123 + (seq % 10), 4)
            for x in range(1, 11)
        ],
        "schema_version": 1,
    }


class ResourceMonitor:
    """Samples CPU and RSS memory usage of a target process tree."""

    def __init__(self, pid: int, interval: float = 0.1):
        self.pid = pid
        self.interval = interval
        self.cpu_samples: list[float] = []
        self.mem_samples_mb: list[float] = []
        self._running = False
        self._task = None

    async def _sample_loop(self):
        try:
            parent = psutil.Process(self.pid)
        except psutil.NoSuchProcess:
            return

        while self._running:
            try:
                # Aggregate process and all child processes
                procs = [parent] + parent.children(recursive=True)
                total_cpu = 0.0
                total_mem = 0.0
                for p in procs:
                    try:
                        total_cpu += p.cpu_percent(interval=None)
                        total_mem += p.memory_info().rss
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass

                self.cpu_samples.append(total_cpu)
                self.mem_samples_mb.append(total_mem / (1024 * 1024))
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break

            await asyncio.sleep(self.interval)

    def start(self):
        self._running = True
        self._task = asyncio.create_task(self._sample_loop())

    async def stop(self) -> tuple[float, float, float, float]:
        self._running = False
        if self._task:
            await asyncio.gather(self._task, return_exceptions=True)

        avg_cpu = float(statistics.mean(self.cpu_samples)) if self.cpu_samples else 0.0
        peak_cpu = float(max(self.cpu_samples)) if self.cpu_samples else 0.0
        avg_mem = float(statistics.mean(self.mem_samples_mb)) if self.mem_samples_mb else 0.0
        peak_mem = float(max(self.mem_samples_mb)) if self.mem_samples_mb else 0.0
        return avg_cpu, peak_cpu, avg_mem, peak_mem


async def wait_for_url(url: str, timeout: float = 30.0) -> bool:
    """Poll URL until it responds with 200 OK or timeout expires."""
    start = time.monotonic()
    async with httpx.AsyncClient(timeout=1.0) as client:
        while time.monotonic() - start < timeout:
            try:
                r = await client.get(url)
                if r.status_code == 200:
                    return True
            except Exception:
                pass
            await asyncio.sleep(0.5)
    return False


async def run_load_test(
    target_url: str,
    total_requests: int,
    concurrency: int,
    test_name: str,
) -> dict[str, Any]:
    """Execute concurrent HTTP POST load test against the specified endpoint."""
    logger.info(f"[{test_name}] Starting benchmark: {total_requests} requests, concurrency={concurrency}")

    # Pre-generate and serialize payloads in memory to eliminate client serialization bias
    payload_bytes = [
        json.dumps(generate_payload(i)).encode("utf-8")
        for i in range(total_requests)
    ]
    latencies_ms: list[float] = []
    success_count = 0
    failure_count = 0

    limits = httpx.Limits(max_keepalive_connections=concurrency * 4, max_connections=concurrency * 4)
    timeout = httpx.Timeout(10.0, connect=5.0)

    # Chunk requests evenly across concurrent workers
    chunk_size = total_requests // concurrency
    rem = total_requests % concurrency

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        start_time = time.monotonic()
        req_headers = {"Content-Type": "application/json"}

        async def worker(start_idx: int, count: int):
            nonlocal success_count, failure_count
            for i in range(start_idx, start_idx + count):
                data = payload_bytes[i]
                t0 = time.monotonic()
                try:
                    resp = await client.post(target_url, content=data, headers=req_headers)
                    t_elapsed = (time.monotonic() - t0) * 1000.0
                    latencies_ms.append(t_elapsed)
                    if resp.status_code in (200, 202):
                        success_count += 1
                    else:
                        failure_count += 1
                except Exception:
                    t_elapsed = (time.monotonic() - t0) * 1000.0
                    latencies_ms.append(t_elapsed)
                    failure_count += 1

        # Launch concurrent workers
        tasks = []
        cur_idx = 0
        for w_id in range(concurrency):
            c = chunk_size + (1 if w_id < rem else 0)
            if c > 0:
                tasks.append(asyncio.create_task(worker(cur_idx, c)))
                cur_idx += c

        await asyncio.gather(*tasks)
        total_duration = time.monotonic() - start_time

    sorted_lat = sorted(latencies_ms) if latencies_ms else [0.0]
    rps = float(total_requests / total_duration) if total_duration > 0 else 0.0

    stats = {
        "engine": test_name,
        "target_url": target_url,
        "total_requests": total_requests,
        "concurrency": concurrency,
        "duration_seconds": round(total_duration, 3),
        "throughput_rps": round(rps, 1),
        "success_count": success_count,
        "failure_count": failure_count,
        "latency_ms": {
            "min": round(float(sorted_lat[0]), 2),
            "p50": round(calc_percentile(sorted_lat, 50), 2),
            "p90": round(calc_percentile(sorted_lat, 90), 2),
            "p95": round(calc_percentile(sorted_lat, 95), 2),
            "p99": round(calc_percentile(sorted_lat, 99), 2),
            "p999": round(calc_percentile(sorted_lat, 99.9), 2),
            "max": round(float(sorted_lat[-1]), 2),
            "mean": round(float(statistics.mean(sorted_lat)), 2),
        },
    }

    logger.info(
        f"[{test_name}] Completed in {total_duration:.2f}s | "
        f"Throughput: {rps:.1f} RPS | p50: {stats['latency_ms']['p50']}ms | "
        f"p95: {stats['latency_ms']['p95']}ms | p99: {stats['latency_ms']['p99']}ms | "
        f"Success: {success_count}/{total_requests}"
    )
    return stats


async def benchmark_service(
    service_cmd: list[str],
    health_url: str,
    target_url: str,
    test_name: str,
    total_requests: int,
    concurrency: int,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Launch process, attach resource monitor, run load test, and collect telemetry."""
    logger.info(f"Starting service for {test_name}: {' '.join(service_cmd)}")
    full_env = os.environ.copy()
    if env:
        full_env.update(env)

    proc = subprocess.Popen(
        service_cmd,
        cwd=str(PROJECT_ROOT),
        env=full_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        is_healthy = await wait_for_url(health_url, timeout=20.0)
        if not is_healthy:
            raise RuntimeError(f"{test_name} failed healthcheck at {health_url}")

        monitor = ResourceMonitor(proc.pid, interval=0.1)
        monitor.start()

        # Warm up with 500 requests
        logger.info(f"[{test_name}] Warming up with 500 requests...")
        await run_load_test(target_url, total_requests=500, concurrency=concurrency, test_name=f"{test_name}-warmup")

        # Run primary benchmark
        stats = await run_load_test(target_url, total_requests=total_requests, concurrency=concurrency, test_name=test_name)

        avg_cpu, peak_cpu, avg_mem, peak_mem = await monitor.stop()
        stats["resource_footprint"] = {
            "avg_cpu_percent": round(avg_cpu, 1),
            "peak_cpu_percent": round(peak_cpu, 1),
            "avg_memory_mb": round(avg_mem, 1),
            "peak_memory_mb": round(peak_mem, 1),
        }
        return stats

    finally:
        logger.info(f"Terminating {test_name} process (PID {proc.pid})...")
        try:
            parent = psutil.Process(proc.pid)
            for child in parent.children(recursive=True):
                child.terminate()
            parent.terminate()
        except Exception:
            pass
        proc.kill()
        proc.wait()
        await asyncio.sleep(1.0)


async def main_async(total_requests: int, concurrency: int):
    results = {
        "benchmark_metadata": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "target_requests": total_requests,
            "concurrency": concurrency,
            "platform": sys.platform,
            "cpu_count": psutil.cpu_count(logical=True),
        },
        "python_ingestor": {},
        "go_ingestor": {},
        "comparison_metrics": {},
    }

    # 1. Benchmark Python Ingestor (Uvicorn + FastAPI)
    python_env = {
        "CACHE_ENABLED": "false",
        "POSTGRES_DSN": "postgresql://mock:mock@127.0.0.1:5432/mock",
        "REDIS_URL": "redis://127.0.0.1:6379/0",
        "API_HOST": "127.0.0.1",
        "API_PORT": "8000",
    }
    python_cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "api.app:create_app",
        "--factory",
        "--host",
        "127.0.0.1",
        "--port",
        "8000",
        "--workers",
        "1",
    ]

    try:
        py_stats = await benchmark_service(
            service_cmd=python_cmd,
            health_url="http://127.0.0.1:8000/health",
            target_url="http://127.0.0.1:8000/api/v1/telemetry/ingest",
            test_name="Python (FastAPI)",
            total_requests=total_requests,
            concurrency=concurrency,
            env=python_env,
        )
        results["python_ingestor"] = py_stats
    except Exception as e:
        logger.error(f"Python benchmark failed: {e}", exc_info=True)
        results["python_ingestor"] = {"error": str(e)}

    # 2. Benchmark Go Ingestor
    go_bin = PROJECT_ROOT / "bin" / ("ingestor.exe" if sys.platform == "win32" else "ingestor")
    if not go_bin.exists():
        logger.info("Building Go ingestor binary...")
        subprocess.run(
            ["go", "build", "-o", str(go_bin), "./cmd/ingestor"],
            cwd=str(PROJECT_ROOT / "ingestor-go"),
            check=True,
        )

    go_env = {
        "HTTP_PORT": "8080",
        "KAFKA_DISPATCH_ENABLED": "false",
        "WORKER_POOL_SIZE": "100",
        "WORKER_BUFFER_SIZE": "100000",
    }
    go_cmd = [str(go_bin)]

    try:
        go_stats = await benchmark_service(
            service_cmd=go_cmd,
            health_url="http://127.0.0.1:8080/health",
            target_url="http://127.0.0.1:8080/api/v1/telemetry/ingest?async=true",
            test_name="Go (Goroutine Pool)",
            total_requests=total_requests,
            concurrency=concurrency,
            env=go_env,
        )
        results["go_ingestor"] = go_stats
    except Exception as e:
        logger.error(f"Go benchmark failed: {e}", exc_info=True)
        results["go_ingestor"] = {"error": str(e)}

    # Compute comparative improvements
    if "throughput_rps" in results["python_ingestor"] and "throughput_rps" in results["go_ingestor"]:
        py_rps = results["python_ingestor"]["throughput_rps"]
        go_rps = results["go_ingestor"]["throughput_rps"]
        py_p99 = results["python_ingestor"]["latency_ms"]["p99"]
        go_p99 = results["go_ingestor"]["latency_ms"]["p99"]
        py_mem = results["python_ingestor"]["resource_footprint"]["peak_memory_mb"]
        go_mem = results["go_ingestor"]["resource_footprint"]["peak_memory_mb"]

        throughput_gain_pct = round(((go_rps - py_rps) / py_rps) * 100, 1) if py_rps > 0 else 0.0
        p99_reduction_pct = round(((py_p99 - go_p99) / py_p99) * 100, 1) if py_p99 > 0 else 0.0
        mem_reduction_pct = round(((py_mem - go_mem) / py_mem) * 100, 1) if py_mem > 0 else 0.0

        results["comparison_metrics"] = {
            "throughput_multiplier": round(go_rps / py_rps, 2) if py_rps > 0 else 0.0,
            "throughput_gain_pct": throughput_gain_pct,
            "p99_latency_reduction_pct": p99_reduction_pct,
            "memory_footprint_reduction_pct": mem_reduction_pct,
        }

    # Persist structured output
    RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(RESULTS_FILE, "w") as f:
        json.dump(results, f, indent=2)

    logger.info(f"Results persisted successfully to {RESULTS_FILE}")
    print("\n" + "=" * 80)
    print("                  INGESTION BENCHMARK COMPARATIVE SUMMARY")
    print("=" * 80)
    print(f"{'Metric':<35} | {'Python (FastAPI)':<18} | {'Go (Goroutines)':<18}")
    print("-" * 80)
    py = results.get("python_ingestor", {})
    go = results.get("go_ingestor", {})
    print(f"{'Throughput (RPS)':<35} | {py.get('throughput_rps', 0):<18} | {go.get('throughput_rps', 0):<18}")
    print(f"{'p50 Latency (ms)':<35} | {py.get('latency_ms', {}).get('p50', 0):<18} | {go.get('latency_ms', {}).get('p50', 0):<18}")
    print(f"{'p95 Latency (ms)':<35} | {py.get('latency_ms', {}).get('p95', 0):<18} | {go.get('latency_ms', {}).get('p95', 0):<18}")
    print(f"{'p99 Latency (ms)':<35} | {py.get('latency_ms', {}).get('p99', 0):<18} | {go.get('latency_ms', {}).get('p99', 0):<18}")
    print(f"{'Peak CPU (%)':<35} | {py.get('resource_footprint', {}).get('peak_cpu_percent', 0):<18} | {go.get('resource_footprint', {}).get('peak_cpu_percent', 0):<18}")
    print(f"{'Peak Memory (MB)':<35} | {py.get('resource_footprint', {}).get('peak_memory_mb', 0):<18} | {go.get('resource_footprint', {}).get('peak_memory_mb', 0):<18}")
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(description="Go vs Python Ingestion Benchmark")
    parser.add_argument("--requests", type=int, default=50000, help="Total requests to send (default: 50,000)")
    parser.add_argument("--concurrency", type=int, default=50, help="Concurrent workers (default: 50)")
    args = parser.parse_args()

    asyncio.run(main_async(args.requests, args.concurrency))


if __name__ == "__main__":
    main()
