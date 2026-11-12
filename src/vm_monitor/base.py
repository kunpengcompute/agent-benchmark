# vm_monitor/base.py
"""
VMMonitorBase - Abstract Base Class for VM Monitor

Provides system-level monitoring infrastructure independent of VMM type:
- NUMA memory statistics
- Hugepage memory tracking
- NUMA CPU usage
- Host CPU/memory statistics
- Swap usage
- Process memory via numastat
- Sampling loop framework
- Export/analysis infrastructure
"""

import csv
import glob
import logging
import os
import queue
import re
import signal
import subprocess
import sys
import threading
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import psutil

from vm_monitor.procfs import PROC_STATUSES, read_cmdline, read_comm_state
from vm_monitor.sampling import HeavyRefreshScheduler, parse_numa_maps

logger = logging.getLogger(__name__)

# Host page size (bytes). Used to convert /proc/vmstat page counters (pgscan,
# pgsteal, workingset_refault) into MiB. Guarded so importing this module on a
# non-Linux host (where SC_PAGE_SIZE is unset) still works for tests.
try:
    _PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")
except (ValueError, OSError, AttributeError):
    _PAGE_SIZE = 4096

# sysconf clock ticks per second for /proc/<pid>/stat utime+stime jiffies.
try:
    _CLK_TCK = os.sysconf("SC_CLK_TCK")
except (ValueError, OSError, AttributeError):
    _CLK_TCK = 100

# Process comm of the userspace ublk daemon (aenv/Cloud Hypervisor snapshot
# I/O backend). collect_ublk_daemon resolves this lazily; on hosts without a
# ublk runtime it is simply absent -> daemon CPU history stays empty.
_UBLK_DAEMON_COMM = "uvm-ublk-daemon"

# Block-layer stat sector size. The kernel documents /sys/block/<dev>/stat
# sector counters as always 512 bytes regardless of the physical sector size.
_SECTOR_SIZE_BYTES = 512
# Conversion factor for byte -> MiB used throughout the monitor.
_BYTES_PER_MIB = 2**20


# Virtual / software block-device prefixes excluded from auto-discovery. These
# either carry no real I/O (loop, ram, sr, zram) or are software layers stacked
# on physical disks (md, dm) that would double-count throughput. ublk is the
# per-sandbox user-space block device a sandbox fleet exposes (hundreds to
# thousands of ublkb* nodes): its I/O is already counted on the backing physical
# disk and on the ublk-daemon CPU collector, and one column set per node blows
# past Excel's 16,384-column limit. Override with an explicit --disks list when
# you want to monitor one of these.
_VIRTUAL_DISK_PREFIXES = ("loop", "ram", "sr", "zram", "md", "dm", "ublk")


# Names of the /proc-based resource collectors collect_sample / _disk_subsample_sleep
# can gate off via --no-X (see vm_monitor/cli.py COLLECTOR_FLAGS). The default
# enabled set is the full set (backward-compat: all collectors on). Internal names
# are the snake_case collector method stems; cli.py maps hyphenated flag stems to these.
_ALL_SAMPLE_COLLECTORS = frozenset(
    {
        "hugepage",
        "numa_cpu",
        "host_stats",
        "swap",
        "host_mem_detail",
        "pressure",
        "numa_memory",
        "vm_total",
        "disk",
        "ublk",
        "pss",
        "rss",
        "ublk_daemon",
        "hostproc",
    }
)

# Concurrency for the per-VM heavy read. Page-table walks are kernel work, so
# threads shrink wall time but not CPU; measured on 192 x 1 GiB 4K-page VMs:
# 4 threads +14% CPU / 3.5x wall, 8 threads +16% / 6.5x, 16 threads +44% /
# 10x, 64 threads +43% / ~13x. 8 is the knee.
_VM_COLLECT_WORKERS = 8


def _discover_block_devices() -> list[str]:
    """Auto-discover physical block devices present on this host.

    Enumerates /sys/block and returns the sorted list of devices whose names do
    not start with a known virtual/software prefix and that expose a readable
    ``stat`` file. Returns ``[]`` on non-Linux hosts or if /sys/block is
    unreadable, so the monitor degrades gracefully (callers may still set
    ``target_disks`` explicitly).
    """
    devices: list[str] = []
    try:
        entries = os.listdir("/sys/block")
    except (OSError, FileNotFoundError):
        return devices
    for name in entries:
        if name.startswith(_VIRTUAL_DISK_PREFIXES):
            continue
        if os.path.exists(f"/sys/block/{name}/stat"):
            devices.append(name)
    return sorted(devices)


def _compute_disk_io_rates(cur: dict, prev: dict, interval: float) -> dict:
    """Compute per-device disk I/O rates from two /sys/block/<dev>/stat snapshots.

    Pure function (no /proc or /sys access) so the delta math is unit-testable
    on non-Linux hosts. Each snapshot is {dev: {sectors_read, sectors_written,
    reads_completed, read_ms, writes_completed, write_ms, ms_io, weighted_ms,
    inflight}}. Returns {dev: {r_mb_s, w_mb_s, util_pct, inflight, r_mb, w_mb,
    avg_queue_depth, read_await_ms, write_await_ms, r_iops, w_iops,
    avg_rq_sz}}.

    Sector size is 512 bytes (kernel block-layer stat sector size). util_pct is
    the fraction of wall-time the device spent servicing I/O: ms_io is the
    cumulative ms the device had >=1 I/O in-flight, so util = delta_ms_io / 1000
    / interval * 100 (busy-seconds / wall-seconds), clamped to [0, 100].
    avg_queue_depth is the mean in-flight request count over the interval
    (weighted_ms / 1000 / interval, where weighted_ms is I/O-time weighted by
    queue depth). read/write_await_ms is mean latency per completed I/O
    (read_ms / reads_completed). r_iops/w_iops are completed read/write I/Os
    per second (delta reads/writes / interval). avg_rq_sz is the iostat-style
    average request size in 512-byte sectors = total sectors / total I/Os over
    the interval. A zero/None interval or missing previous snapshot yields
    zero rates (first sample has no baseline). Snapshot fields are read via
    .get so partial snapshots (older callers) do not raise.
    """
    rates = {}
    if not cur or not prev or not interval or interval <= 0:
        for dev in cur or {}:
            rates[dev] = {
                "r_mb_s": 0.0,
                "w_mb_s": 0.0,
                "util_pct": 0.0,
                "inflight": (cur[dev] or {}).get("inflight", 0),
                "r_mb": 0.0,
                "w_mb": 0.0,
                "avg_queue_depth": 0.0,
                "read_await_ms": 0.0,
                "write_await_ms": 0.0,
                "r_iops": 0.0,
                "w_iops": 0.0,
                "avg_rq_sz": 0.0,
            }
        return rates
    for dev, c in cur.items():
        p = prev.get(dev, {})
        d_read = c.get("sectors_read", 0) - p.get("sectors_read", 0)
        d_write = c.get("sectors_written", 0) - p.get("sectors_written", 0)
        d_ms = c.get("ms_io", 0) - p.get("ms_io", 0)
        r_mb = d_read * _SECTOR_SIZE_BYTES / _BYTES_PER_MIB
        w_mb = d_write * _SECTOR_SIZE_BYTES / _BYTES_PER_MIB
        util = max(0.0, min(100.0, d_ms / 1000.0 / interval * 100))
        d_reads = c.get("reads_completed", 0) - p.get("reads_completed", 0)
        d_writes = c.get("writes_completed", 0) - p.get("writes_completed", 0)
        d_read_ms = c.get("read_ms", 0) - p.get("read_ms", 0)
        d_write_ms = c.get("write_ms", 0) - p.get("write_ms", 0)
        d_weighted_ms = c.get("weighted_ms", 0) - p.get("weighted_ms", 0)
        rates[dev] = {
            "r_mb_s": round(r_mb / interval, 2),
            "w_mb_s": round(w_mb / interval, 2),
            "util_pct": round(util, 1),
            "inflight": c.get("inflight", 0),
            "r_mb": round(r_mb, 2),
            "w_mb": round(w_mb, 2),
            "avg_queue_depth": round(d_weighted_ms / 1000.0 / interval, 3),
            "read_await_ms": round(d_read_ms / d_reads, 3) if d_reads > 0 else 0.0,
            "write_await_ms": round(d_write_ms / d_writes, 3) if d_writes > 0 else 0.0,
            "r_iops": round(d_reads / interval, 2),
            "w_iops": round(d_writes / interval, 2),
            "avg_rq_sz": round((d_read + d_write) / (d_reads + d_writes), 2) if (d_reads + d_writes) > 0 else 0.0,
        }
    return rates


def _compute_pressure_rates(cur: dict, prev: dict, interval: float, page_size: int = _PAGE_SIZE) -> dict:
    """Compute page-cache pressure rates (MiB/s) from two /proc/vmstat snapshots.

    Pure function (no /proc access) so the math is unit-testable on non-Linux
    hosts. Each snapshot holds cumulative page counters:
    pgscan_kswapd / pgscan_direct / pgscan_direct_throttle (pages scanned for
    reclaim), pgsteal_kswapd / pgsteal_direct (pages reclaimed), and
    workingset_refault_file (evicted file pages refaulted back). Returns rates
    in MiB/s: page_scan_mib_s, page_reclaim_mib_s, file_refault_mib_s, plus
    direct_reclaim_mib_s (the blocking subset -- direct + throttle, i.e. pages
    reclaimed by a synchronously-stalled allocator, not background kswapd) and
    allocstall_per_s (allocation requests that stalled waiting on reclaim,
    straight count/s -- the closest kernel proxy for "processes blocked on
    memory reclaim"). allocstall is summed across the per-zone-type variants
    (allocstall_normal/_dma32/...) -- modern /proc/vmstat exposes no plain sum.
    A zero/None interval or missing previous snapshot yields zero rates.
    """
    rates = {
        "page_scan_mib_s": 0.0,
        "page_reclaim_mib_s": 0.0,
        "file_refault_mib_s": 0.0,
        "direct_reclaim_mib_s": 0.0,
        "allocstall_per_s": 0.0,
    }
    if not cur or not prev or not interval or interval <= 0:
        return rates
    mib_per_page = page_size / _BYTES_PER_MIB

    def _delta(key):
        return max(0, cur.get(key, 0) - prev.get(key, 0))

    scan = _delta("pgscan_kswapd") + _delta("pgscan_direct") + _delta("pgscan_direct_throttle")
    steal = _delta("pgsteal_kswapd") + _delta("pgsteal_direct")
    refault = _delta("workingset_refault_file")
    # Direct reclaim = the allocator blocked itself doing reclaim (direct scan)
    # or was put to sleep because too many direct reclaimers were already
    # running (throttle). Excludes kswapd (background, non-blocking).
    direct = _delta("pgscan_direct") + _delta("pgscan_direct_throttle")
    # allocstall is exposed per zone-type (allocstall_dma/_dma32/_normal/
    # _movable/_high/_device) on modern kernels -- /proc/vmstat has NO plain
    # "allocstall" sum field. Sum every allocstall_* variant present in either
    # snapshot so the rate is correct regardless of which zones the kernel
    # config exposes; fall back to a plain "allocstall" only for legacy kernels
    # that exposed a single sum (and neither variants nor a plain field exist).
    allocstall_keys = {k for k in set(cur) | set(prev) if k.startswith("allocstall_")}
    if allocstall_keys:
        allocstall = sum(max(0, cur.get(k, 0) - prev.get(k, 0)) for k in allocstall_keys)
    else:
        allocstall = _delta("allocstall")
    rates["page_scan_mib_s"] = round(scan * mib_per_page / interval, 3)
    rates["page_reclaim_mib_s"] = round(steal * mib_per_page / interval, 3)
    rates["file_refault_mib_s"] = round(refault * mib_per_page / interval, 3)
    rates["direct_reclaim_mib_s"] = round(direct * mib_per_page / interval, 3)
    rates["allocstall_per_s"] = round(allocstall / interval, 3)
    return rates


class _ProcRawWriter:
    """Background raw-snapshot dumper — every /proc-style source the monitor reads.

    Files are named by SCOPE so the dimension is obvious without context:
      host_mem_raw.csv       wide   whole-machine /proc/meminfo (MB; _read_meminfo
                                   converts kB->MB before dump, NOT raw kB)
      host_vmstat_raw.csv    wide   whole-machine /proc/vmstat (page counters)
      host_cpu_raw.csv       wide   whole-machine /proc/stat aggregate cpu line
                                   (user/nice/.../iowait jiffies + ctxt + softirq
                                   + procs_running/blocked) — intr line skipped
      host_percpu_raw.csv    long   every /proc/stat cpuN line (one row per cpu
                                   per sample; long avoids a ~2000-col sheet)
      numa_cpu_map.csv       static node->cpu mapping, written ONCE (boot-constant;
                                   /sys/devices/system/node/nodeN/cpulist). Join with
                                   host_percpu_raw.csv to rebuild per-NUMA CPU%
      vm_cpu_raw.csv         long   per-VM /proc/<pid>/stat utime+stime jiffies
                                   (one row per VM per sample). Raw cumulative
                                   counters; agent derives per-VM CPU% via delta
                                   over the monotonic interval. memory_mb is the
                                   numa_maps total already collected per VM
                                   (gauge, no extra /proc read).
      numa_mem_raw.csv       long   per-NUMA-node /sys/devices/system/node/
                                   nodeN/meminfo, one row per node per sample
                                   (MB). Same dicts get_numa_nodes_memory()
                                   already parses -- no extra read.
      disk_io_raw.csv        long   per-device /sys/block/<dev>/stat raw counters
                                   (one row per device per 1s sub-sample). 17
                                   standard fields incl. discards/flush; agent
                                   derives r/w MB-s, util%, await, iops via delta.
      ublk_cpu_raw.csv       long   ublk-daemon /proc/<pid>/stat utime+stime raw
                                   jiffies (one row per daemon per 1s sub-sample).
                                   Raw cumulative counters; agent derives ublk-daemon
                                   CPU% via delta -- the derived 'cores' rate in
                                   ublk_daemon_history alone is lossy. A pid restart
                                   (new daemon) is just another row keyed by pid.

    collect_sample enqueues the dicts already read for the swap / mem-detail /
    pressure collectors (meminfo, vmstat) plus a freshly parsed /proc/stat and the
    per-VM pid list; a daemon thread drains the queue and appends rows so the hot
    sampling path never blocks on file I/O. meminfo/vmstat/stat-aggregate are wide
    (one row per sample); per-cpu and per-VM are long (one row per cpu/vm per
    sample). numa_cpu_map is one-shot (idempotent: only the first enqueue writes).
    All files land in a ``raw_data/`` subdir of the monitor log_dir so the raw
    snapshots stay separated from resource_report.xlsx and the xlsx-overflow
    CSVs in the parent dir.

    Header is written from the first non-empty row and later rows align to it
    (missing -> '', extra -> ignored). ponytail: a field appearing mid-run (kernel
    module load) would be dropped; these sets are stable per boot, so never in
    practice.
    """

    # /proc/stat "cpu" line positional fields (stable since 2.6).
    _CPU_FIELDS = ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal", "guest", "guest_nice")

    # /sys/block/<dev>/stat positional fields (kernel docs Documentation/admin-guide/iostats.rst).
    # 0-10 stable since 2.6; 11-14 discards since 4.18; 15-16 flush since 5.5.
    # Older kernels return fewer fields -> the writer pads missing ones with "".
    _DISK_FIELDS = (
        "reads_completed",
        "reads_merged",
        "sectors_read",
        "read_ms",
        "writes_completed",
        "writes_merged",
        "sectors_written",
        "write_ms",
        "inflight",
        "ms_io",
        "weighted_ms",
        "discards_completed",
        "discards_merged",
        "sectors_discarded",
        "discard_ms",
        "flush_requests",
        "flush_ms",
    )

    def __init__(self, log_dir: str):
        # Raw snapshots live in a raw_data/ subdir so the *_raw.csv +
        # numa_cpu_map.csv files stay separated from resource_report.xlsx and
        # the xlsx-overflow CSVs in the parent log_dir.
        self._dir = os.path.join(log_dir, "raw_data")
        os.makedirs(self._dir, exist_ok=True)
        self._q: queue.SimpleQueue = queue.SimpleQueue()
        self._wide: dict = {}  # key -> (fh, DictWriter)
        self._percpu = None  # (fh, DictWriter)
        self._vm = None  # (fh, DictWriter)
        self._disk = None  # (fh, DictWriter)
        self._ublk = None  # (fh, DictWriter)
        self._hostproc = None  # (fh, DictWriter)
        self._numa_mem = None  # (fh, DictWriter)
        self._numa_map_written = False
        self._thread = threading.Thread(target=self._run, name="proc-raw-writer", daemon=True)
        self._thread.start()

    # --- producers (collect_sample / _disk_subsample_sleep call these; O(1) puts) ---
    def enqueue_proc(self, ts: str, meminfo: dict, vmstat: dict, stat: dict) -> None:
        self._q.put(("proc", ts, meminfo, vmstat, stat))

    def enqueue_vm(self, ts: str, vms: list) -> None:
        self._q.put(("vm", ts, vms))

    def enqueue_disk(self, ts: str, dev_raw: dict) -> None:
        """dev_raw = {dev: [field0, field1, ...]} raw /sys/block/<dev>/stat splits."""
        self._q.put(("disk", ts, dev_raw))

    def enqueue_ublk(self, ts: str, pid: int, utime, stime) -> None:
        """ublk-daemon raw jiffies (utime+stime) for ublk_cpu_raw.csv."""
        self._q.put(("ublk", ts, pid, utime, stime))

    def enqueue_hostproc(self, ts: str, name: str, pid: int, utime, stime, pss_mb) -> None:
        """Named host-process raw jiffies (utime+stime) + PSS MB for
        hostproc_cpu_raw.csv. ``name`` is the spec label (comm)."""
        self._q.put(("hostproc", ts, name, pid, utime, stime, pss_mb))

    def enqueue_numa_map(self, node_to_cpus: dict) -> None:
        # Idempotent: collect_sample calls every cycle; only the first writes.
        if self._numa_map_written:
            return
        self._q.put(("numa_map", node_to_cpus))

    def enqueue_numa_mem(self, ts: str, nodes: list) -> None:
        """Per-NUMA-node meminfo rows (already parsed by get_numa_nodes_memory)."""
        self._q.put(("numa_mem", ts, nodes))

    # --- drain thread ---
    def _run(self) -> None:
        while True:
            item = self._q.get()
            if item is None:  # close sentinel -> drain done, exit
                break
            tag = item[0]
            if tag == "proc":
                _, ts, meminfo, vmstat, stat = item
                if meminfo:
                    self._write_wide("meminfo", "host_mem_raw.csv", ts, meminfo)
                if vmstat:
                    self._write_wide("vmstat", "host_vmstat_raw.csv", ts, vmstat)
                if stat:
                    self._write_stat(ts, stat)
            elif tag == "vm":
                _, ts, vms = item
                self._write_vm(ts, vms)
            elif tag == "disk":
                _, ts, dev_raw = item
                self._write_disk(ts, dev_raw)
            elif tag == "ublk":
                _, ts, pid, utime, stime = item
                self._write_ublk(ts, pid, utime, stime)
            elif tag == "hostproc":
                _, ts, name, pid, utime, stime, pss_mb = item
                self._write_hostproc(ts, name, pid, utime, stime, pss_mb)
            elif tag == "numa_map":
                _, node_to_cpus = item
                self._write_numa_map(node_to_cpus)
            elif tag == "numa_mem":
                _, ts, nodes = item
                self._write_numa_mem(ts, nodes)

    def _write_wide(self, key: str, fname: str, ts: str, row: dict) -> None:
        """Append one wide row; open file + write header from the first row."""
        sink = self._wide.get(key)
        if sink is None:
            fh = open(os.path.join(self._dir, fname), "w", newline="", encoding="utf-8")
            dw = csv.DictWriter(fh, fieldnames=["timestamp", *row.keys()], restval="", extrasaction="ignore")
            dw.writeheader()
            sink = (fh, dw)
            self._wide[key] = sink
        fh, dw = sink
        dw.writerow({"timestamp": ts, **row})
        fh.flush()  # line-buffered: rows land on disk promptly for crash safety

    def _write_stat(self, ts: str, stat: dict) -> None:
        # Aggregate cpu + ctxt/btime/procs/softirq -> one wide row per sample.
        cpu = stat.get("cpu") or []
        softirq = stat.get("softirq") or []
        agg: dict = {}
        for i, name in enumerate(self._CPU_FIELDS):
            agg[f"cpu_{name}"] = cpu[i] if i < len(cpu) else ""
        for k in ("ctxt", "btime", "processes", "procs_running", "procs_blocked"):
            agg[k] = stat.get(k, "")
        for i, v in enumerate(softirq):
            agg["softirq_total" if i == 0 else f"softirq_{i}"] = v
        self._write_wide("stat", "host_cpu_raw.csv", ts, agg)

        # Per-cpu -> long format (one row per cpu per sample).
        cpus = stat.get("cpus") or {}
        if not cpus:
            return
        if self._percpu is None:
            fh = open(os.path.join(self._dir, "host_percpu_raw.csv"), "w", newline="", encoding="utf-8")
            dw = csv.DictWriter(
                fh, fieldnames=["timestamp", "cpu_id", *self._CPU_FIELDS], restval="", extrasaction="ignore"
            )
            dw.writeheader()
            self._percpu = (fh, dw)
        fh, dw = self._percpu
        for cpu_id, vals in sorted(cpus.items(), key=lambda kv: int(kv[0])):
            row = {"timestamp": ts, "cpu_id": cpu_id}
            for i, name in enumerate(self._CPU_FIELDS):
                row[name] = vals[i] if i < len(vals) else ""
            dw.writerow(row)
        fh.flush()

    def _write_vm(self, ts: str, vms: list) -> None:
        # Per-VM -> long format (one row per VM per sample). pss_mb/rss_mb are
        # gauges (current MB via smaps_rollup / /proc/<pid>/status VmRSS, already
        # collected by _collect_single_vm); utime/stime are cumulative counters
        # an agent deltas into CPU cores.
        if self._vm is None:
            fh = open(os.path.join(self._dir, "vm_cpu_raw.csv"), "w", newline="", encoding="utf-8")
            dw = csv.DictWriter(
                fh,
                fieldnames=["timestamp", "pid", "vm_name", "utime", "stime", "pss_mb", "rss_mb", "memory_mb"],
                restval="",
                extrasaction="ignore",
            )
            dw.writeheader()
            self._vm = (fh, dw)
        fh, dw = self._vm
        for v in vms:
            ut, st = v.get("utime"), v.get("stime")
            if ut is None or st is None:
                continue  # VM exited or /proc/<pid>/stat unreadable mid-sample
            dw.writerow(
                {
                    "timestamp": ts,
                    "pid": v.get("pid"),
                    "vm_name": v.get("name", ""),
                    "utime": ut,
                    "stime": st,
                    "pss_mb": v.get("pss_mb", ""),
                    "rss_mb": v.get("rss_mb", ""),
                    "memory_mb": v.get("memory_mb", ""),
                }
            )
        fh.flush()

    def _write_disk(self, ts: str, dev_raw: dict) -> None:
        # Per-device -> long format (one row per device per 1s sub-sample).
        # Raw /sys/block/<dev>/stat counters; agent derives r_mb_s / util / await
        # via delta. Missing trailing fields (older kernel) pad to "".
        if self._disk is None:
            fh = open(os.path.join(self._dir, "disk_io_raw.csv"), "w", newline="", encoding="utf-8")
            dw = csv.DictWriter(
                fh, fieldnames=["timestamp", "device", *self._DISK_FIELDS], restval="", extrasaction="ignore"
            )
            dw.writeheader()
            self._disk = (fh, dw)
        fh, dw = self._disk
        for dev, fields in sorted(dev_raw.items()):
            row = {"timestamp": ts, "device": dev}
            for i, name in enumerate(self._DISK_FIELDS):
                row[name] = fields[i] if i < len(fields) else ""
            dw.writerow(row)
        fh.flush()

    def _write_numa_map(self, node_to_cpus: dict) -> None:
        # Static boot-constant topology: one row per (node, cpu). Agent groupby
        # rebuilds per-NUMA CPU% from host_percpu_raw.csv.
        if self._numa_map_written:
            return
        path = os.path.join(self._dir, "numa_cpu_map.csv")
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                dw = csv.writer(f)
                dw.writerow(["node", "cpu"])
                for node in sorted(node_to_cpus):
                    for cpu in node_to_cpus[node]:
                        dw.writerow([node, cpu])
        except OSError:
            return  # leave flag False -> a later cycle retries (was: finally set True)
        self._numa_map_written = True

    # Per-NUMA-node meminfo fields dumped for each sample (MB, from
    # get_numa_nodes_memory's already-parsed /sys node meminfo).
    _NUMA_MEM_FIELDS = (
        "total_mb",
        "free_mb",
        "available_mb",
        "swap_cached_mb",
        "active_mb",
        "inactive_mb",
        "anon_pages_mb",
        "file_pages_mb",
        "used_mb",
        "usage_pct",
    )

    def _write_numa_mem(self, ts: str, nodes: list) -> None:
        # Long format: one row per NUMA node per sample. Gauge values (no delta).
        if self._numa_mem is None:
            fh = open(os.path.join(self._dir, "numa_mem_raw.csv"), "w", newline="", encoding="utf-8")
            dw = csv.DictWriter(
                fh,
                fieldnames=["timestamp", "node", *self._NUMA_MEM_FIELDS],
                restval="",
                extrasaction="ignore",
            )
            dw.writeheader()
            self._numa_mem = (fh, dw)
        fh, dw = self._numa_mem
        for node in nodes:
            dw.writerow(
                {
                    "timestamp": ts,
                    "node": node.get("node"),
                    **{k: node.get(k, "") for k in self._NUMA_MEM_FIELDS},
                }
            )
        fh.flush()

    def _write_ublk(self, ts: str, pid, utime, stime) -> None:
        # Per-daemon -> long format (one row per daemon per 1s sub-sample).
        if self._ublk is None:
            fh = open(os.path.join(self._dir, "ublk_cpu_raw.csv"), "w", newline="", encoding="utf-8")
            dw = csv.DictWriter(
                fh, fieldnames=["timestamp", "pid", "utime", "stime"], restval="", extrasaction="ignore"
            )
            dw.writeheader()
            self._ublk = (fh, dw)
        fh, dw = self._ublk
        dw.writerow({"timestamp": ts, "pid": pid, "utime": utime, "stime": stime})
        fh.flush()

    def _write_hostproc(self, ts: str, name: str, pid, utime, stime, pss_mb) -> None:
        # Per named host process -> long format (one row per process per 1s
        # sub-sample). pss_mb is a smaps_rollup gauge; utime/stime are
        # cumulative counters an agent deltas into CPU cores. The ``name``
        # column is the spec label (comm); a pid restart is just another row
        # keyed by the new pid -- never delta ACROSS pids (raw_report groups
        # by (name, pid) for the cores delta, then merges onto one name curve).
        if self._hostproc is None:
            fh = open(os.path.join(self._dir, "hostproc_cpu_raw.csv"), "w", newline="", encoding="utf-8")
            dw = csv.DictWriter(
                fh,
                fieldnames=["timestamp", "name", "pid", "utime", "stime", "pss_mb"],
                restval="",
                extrasaction="ignore",
            )
            dw.writeheader()
            self._hostproc = (fh, dw)
        fh, dw = self._hostproc
        dw.writerow({"timestamp": ts, "name": name, "pid": pid, "utime": utime, "stime": stime, "pss_mb": pss_mb})
        fh.flush()

    def close(self) -> None:
        """Signal the drain thread to exit, then close every open file handle.

        join() with no timeout: a bounded join(timeout=5) closes handles mid-write
        when a single row takes longer than the timeout, losing the row. The drain
        thread exits as soon as it drains the queue to the None sentinel, so this
        only blocks for as long as the queued writes themselves take."""
        self._q.put(None)
        self._thread.join()
        for fh, _ in self._wide.values():
            try:
                fh.close()
            except OSError:
                pass
        for sink in (self._percpu, self._vm, self._disk, self._ublk, self._hostproc, self._numa_mem):
            if sink is not None:
                try:
                    sink[0].close()
                except OSError:
                    pass


# Module-level exposure of the /sys/block/<dev>/stat field set so the
# derived-rate consumer (raw_report._derive_disk) imports the single source of
# truth rather than re-hardcoding the 17-field tuple.
_DISK_FIELDS = _ProcRawWriter._DISK_FIELDS


