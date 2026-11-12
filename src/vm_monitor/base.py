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


class VMMonitorBase(ABC):
    """Abstract Base Class for VM Real-time Monitor

    Provides system-level monitoring infrastructure independent of VMM type.
    Subclasses must implement abstract methods for VMM-specific logic.

    Attributes:
        running: Monitoring state flag
        data: List of collected sample records
        target_numa_nodes: NUMA nodes to monitor for CPU stats
        numa_memory_history: NUMA memory usage timeline (full meminfo)
        hugepage_per_numa: Per-NUMA hugepage statistics
        host_cpu_history: Host total CPU timeline
        swap_history: Swap usage timeline
        vm_total_memory_history: VM total memory aggregation timeline
    """

    # Designated "remote borrowing" NUMA node — the node this platform uses to
    # borrow memory from a remote socket (NUMA5 on the reference 4-socket box).
    # Overridable from the CLI via --remote-numa. The instance attribute
    # (self.remote_numa_id) holds the effective value; set in __init__.
    DEFAULT_REMOTE_NUMA_ID = 5

    # Fields to extract from per-NUMA meminfo (sysfs name -> dict key)
    _NUMA_MEMINFO_FIELDS = {
        "MemTotal": "total_mb",
        "MemFree": "free_mb",
        "MemAvailable": "available_mb",
        "SwapCached": "swap_cached_mb",
        "Active": "active_mb",
        "Inactive": "inactive_mb",
        "AnonPages": "anon_pages_mb",
        "FilePages": "file_pages_mb",
    }

    def __init__(self):
        """Initialize monitor with empty data containers"""
        self.running = False
        self.data = []
        self.stop_event = threading.Event()
        # pid -> (utime+stime ticks, monotonic ts) of the previous sample, the
        # baseline for per-VM CPU%. Replaces psutil.Process.cpu_percent(): the
        # jiffies read is already needed for vm_cpu_raw.csv, so one /proc read
        # now serves both and no psutil object is created per VM.
        self._proc_cpu_prev = {}
        self.numa_memory_history = []
        self.peak_total_memory_mb = 0.0
        self.peak_total_cpu = 0.0
        self.hugepage_total_mb = 0.0
        self.hugepage_free_mb = 0.0
        self.hugepage_used_mb = 0.0
        self.hugepage_used_history = []
        self.peak_hugepage_used_mb = 0.0
        self.last_vm_count = 0

        # Per-NUMA Node Hugepage Statistics
        self.hugepage_per_numa = {}
        self.hugepage_per_numa_history = []

        # Host Machine Total Resource Statistics
        self.host_cpu_history = []
        self.host_mem_history = []
        self.peak_host_cpu = 0.0
        self.peak_host_mem_mb = 0.0

        # Swap Statistics
        self.swap_history = []
        self.peak_swap_used_mb = 0.0
        self.peak_swap_cached_mb = 0.0
        self._last_pswpin = None
        self._last_pswpout = None
        # monotonic timestamp of the last /proc/vmstat read, so swap in/out
        # rates divide by the ACTUAL elapsed window (not the nominal sampling
        # interval). Same pattern as _last_disk_monotonic; not sub-sampled.
        self._last_swap_monotonic = None

        # VM Total Memory Aggregation
        self.vm_total_memory_history = []

        # Sampling interval (set by start_monitoring / wait_for_stress_and_monitor;
        # initialized defensively so delta-based collectors are safe before a loop runs)
        self.interval = 0

        # Disk I/O Statistics (per-device /sys/block/<dev>/stat deltas).
        # Defaults to every physical block device on the host; override from the
        # CLI via --disks (or set target_disks directly) to restrict to a subset.
        self.target_disks = _discover_block_devices()
        self.disk_history = []
        self._last_diskstats = None  # {dev: {sectors_read, sectors_written, ms_io, inflight}}
        # monotonic timestamp of the last /sys/block/<dev>/stat read, so disk
        # rates divide by the ACTUAL elapsed window (not the nominal sampling
        # interval). Disk is sub-sampled at 1s regardless of self.interval.
        self._last_disk_monotonic = None
        self.peak_disk_write_mb_s = 0.0

        # Host Memory Detail (cached / buffers / dirty / writeback from /proc/meminfo)
        self.host_mem_detail_history = []
        self.peak_dirty_mb = 0.0
        self.peak_writeback_mb = 0.0

        # ublk device count (/dev/ublkb*)
        self.ublk_history = []
        self.peak_ublk_devices = 0

        # uvm-ublk-daemon CPU (cores) -- the host-side daemon driving ublk I/O.
        # pid resolved lazily on first collect_ublk_daemon(); -1 = not found
        # (no ublk runtime on this host -> history stays empty -> sheet omits).
        self.ublk_daemon_history = []
        self._ublk_daemon_pid = None
        self._prev_ublk_daemon_jiffies = None
        self._ublk_daemon_last_t = 0.0

        # User-specified host processes to monitor by name (e.g. the agentENV
        # "server" daemon, comm="server" + cmdline contains "release/server").
        # Generalized sibling of collect_ublk_daemon: each spec tracks ONE pid
        # (resolved lazily via _resolve_hostproc_pid, re-resolved on miss) and
        # streams raw utime+stime+pss to hostproc_cpu_raw.csv. ``specs`` is a
        # list of (label, comm, cmdline_substr); set via set_host_proc_specs()
        # from the CLI --host-proc flag. Empty by default -> no-op collector.
        self.hostproc_history: list[dict] = []
        self._hostproc_specs: list[tuple[str, str, str | None]] = []
        self._hostproc_pids: dict[str, int | None] = {}
        self._hostproc_prev: dict[str, float] = {}  # prev jiffies (utime+stime)
        self._hostproc_last_t: dict[str, float] = {}  # monotonic ts of last sample
        # per-spec once-only flags: warned_multi (multi-match), warned_missing
        # (consecutive not-found after threshold). Re-armed when pid is found.
        self._hostproc_warned_multi: set[str] = set()
        self._hostproc_warned_missing: set[str] = set()
        self._hostproc_missing_streak: dict[str, int] = {}  # consecutive miss count
        self._hostproc_ever_seen: set[str] = set()

        # Host page-cache pressure + runnable/blocked procs (from /proc/vmstat
        # + /proc/meminfo + /proc/stat). One sample per collect_sample cycle.
        self.host_pressure_history = []
        self.peak_page_scan_mib_s = 0.0
        self.peak_page_reclaim_mib_s = 0.0
        self.peak_file_refault_mib_s = 0.0
        self._last_pressure = None  # cumulative vmstat page counters
        # monotonic timestamp of the last /proc/vmstat read, so page-scan /
        # reclaim / refault rates divide by ACTUAL elapsed (not self.interval).
        self._last_pressure_monotonic = None
        self._last_cpu_total = None  # cumulative /proc/stat "cpu" total jiffies
        self._last_cpu_iowait = None  # cumulative /proc/stat iowait jiffies
        # Dirty-writeback throttle thresholds (MB); read lazily once from
        # /proc/sys/vm. Used as a threshold line on the Dirty time curve.
        self.dirty_limit_mb = 0.0
        self.dirty_background_limit_mb = 0.0
        self._dirty_limits_read = False

        # Specified NUMA Node CPU Statistics
        self.target_numa_nodes = [0]
        self.numa_cpu_history = defaultdict(list)
        self.numa_cpu_peak = defaultdict(float)
        self.available_numa_nodes = self.get_available_numa_nodes()
        # Effective remote-borrowing node; override from CLI --remote-numa.
        self.remote_numa_id = self.DEFAULT_REMOTE_NUMA_ID

        # Per-collector enable set. Default = every collector on (backward-compat).
        # --no-X flags (forwarded by bench_core monitor.skip) remove names here via
        # disable_collectors(); _collects() gates each call in collect_sample /
        # _disk_subsample_sleep. A disabled collector leaves its history empty so
        # every downstream consumer (exporters / svg_exporter / realtime table /
        # summary) already degrades to omitting that sheet/SVG (verified: no
        # unguarded consumers exist).
        self.enabled_collectors: set[str] = set(_ALL_SAMPLE_COLLECTORS)

        # Output directory (set by the CLI before monitoring starts) and the
        # always-on raw dumper. The dumper writes full meminfo/vmstat/stat +
        # per-VM jiffies + per-disk counters to <log_dir>/{host,vm,disk,numa}_*.csv
        # on a background thread so the sampling loop never blocks on file I/O.
        # Started lazily on the first collect_sample() call once log_dir is known
        # (disk raw enqueues from _disk_subsample_sleep find it already started);
        # joined at monitoring end.
        self.log_dir: str | None = None
        self._proc_raw: _ProcRawWriter | None = None
        # node -> [cpu ids] topology cache (boot-constant); built lazily once
        # and shared by collect_numa_cpu (per-NUMA CPU%) + the raw dumper
        # (numa_cpu_map.csv) so /sys/.../cpulist is read once per run, not per
        # cycle. None until first use.
        self._numa_cpu_map: dict[int, list[int]] | None = None

        # ---- Heavy per-VM memory refresh (numa_maps + smaps_rollup) --------
        # Both reads walk the target process's page tables (~35 ms per GiB
        # with 4 KiB pages, ~12 ms per GiB THP-backed, measured per VM at a
        # 192-VM fleet), so a large fleet cannot afford them every sample. The
        # scheduler spreads a full pass over several samples inside a CPU
        # budget and carries unrefreshed values forward; its policy lives in
        # vm_monitor.sampling so an alternative cadence is a drop-in
        # replacement rather than an edit here.
        self.heavy_refresh = HeavyRefreshScheduler()

    def _collects(self, name: str) -> bool:
        """True if collector ``name`` is currently enabled."""
        return name in self.enabled_collectors

    def disable_collectors(self, names) -> None:
        """Disable the named /proc collectors. Unknown names are silently ignored
        here (the CLI/YAML layer warns before calling, so a stray token never
        reaches this method in practice)."""
        self.enabled_collectors -= {n for n in names if n in _ALL_SAMPLE_COLLECTORS}

    # ==================== Abstract Methods ====================
    @abstractmethod
    def get_vms_realtime(self) -> list[dict]:
        """Get real-time information for all VMs of this VMM type.

        Returns:
            List of dicts with VM info including:
            - pid: Process ID
            - name: VM name
            - cpu_percent: CPU usage percentage
            - memory_mb: Total memory in MB
            - memory_huge_mb: Hugepage memory in MB
            - memory_private_mb: Private memory in MB
            - memory_heap_mb: Heap memory in MB
            - memory_per_numa: Per-NUMA memory breakdown
            - status: Process status
        """
        pass

    @abstractmethod
    def get_process_names(self) -> tuple[str, ...]:
        """Get the process names to monitor for this VMM type.

        Returns:
            Tuple of process name strings (e.g., ('qemu-kvm', 'qemu-system'))
        """
        pass

    @abstractmethod
    def extract_vm_id(self, pid: int, cmdline: str) -> str:
        """Extract VM identifier from process command line.

        Args:
            pid: Process ID
            cmdline: Full command line string

        Returns:
            VM identifier string (e.g., VM name)
        """
        pass

    @abstractmethod
    def get_monitor_title(self) -> str:
        """Get the title for this monitor type.

        Returns:
            Title string (e.g., "QEMU VM Real-time Monitoring")
        """
        pass

    @abstractmethod
    def get_no_vm_message(self) -> str:
        """Get the message to display when no VMs are detected.

        Returns:
            Message string (e.g., "No running QEMU virtual machines detected")
        """
        pass

    @abstractmethod
    def get_csv_filename_prefix(self) -> str:
        """Get the CSV filename prefix for this monitor type.

        Returns:
            Prefix string (e.g., "qemu_monitor")
        """
        pass

    # ==================== NUMA Memory Statistics ====================
    def get_numa_nodes_memory(self):
        """Collect full per-NUMA meminfo from /sys/devices/system/node/node{N}/meminfo

        Parses MemTotal, MemFree, MemAvailable, SwapCached, Active, Inactive,
        AnonPages, FilePages for each NUMA node. Computes used_mb and usage_pct
        from total - free. Preserves backward-compat alias keys (total, used,
        free, usage).
        """
        numa_nodes = []
        try:
            node_dirs = [d for d in os.listdir("/sys/devices/system/node/") if d.startswith("node") and d[4:].isdigit()]
            for node in sorted(node_dirs, key=lambda x: int(x[4:])):
                node_id = int(node[4:])
                path = f"/sys/devices/system/node/{node}/meminfo"
                with open(path) as f:
                    lines = f.read().splitlines()

                # Parse all relevant fields using regex
                # Format: "Node N FieldName:     value kB"
                parsed_mb = {}
                for line in lines:
                    match = re.match(r"Node\s+\d+\s+(\w+):\s+(\d+)\s+kB", line)
                    if match:
                        field_name = match.group(1)
                        value_kb = int(match.group(2))
                        value_mb = round(value_kb / 1024, 2)
                        parsed_mb[field_name] = value_mb

                # Build result dict with canonical keys
                result = {"node": node_id}
                for sysfs_name, dict_key in self._NUMA_MEMINFO_FIELDS.items():
                    result[dict_key] = parsed_mb.get(sysfs_name, 0.0)

                # Compute derived fields
                total_mb = result.get("total_mb", 0.0)
                free_mb = result.get("free_mb", 0.0)
                used_mb = round(total_mb - free_mb, 2)
                usage_pct = round(used_mb / total_mb * 100, 2) if total_mb > 0 else 0.0
                result["used_mb"] = used_mb
                result["usage_pct"] = usage_pct

                # Backward-compat aliases (no suffix)
                result["total"] = total_mb
                result["used"] = used_mb
                result["free"] = free_mb
                result["usage"] = usage_pct

                numa_nodes.append(result)
        except Exception:
            pass

        self.numa_memory_history.append({"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "nodes": numa_nodes})
        return numa_nodes

    def collect_hugepage_stats(self):
        """Collect hugepage memory statistics (total and per-NUMA node)

        Note: /proc/meminfo only reports default hugepage size statistics.
        To get accurate totals across all hugepage sizes, we calculate from per-NUMA stats.
        """
        # First collect per-NUMA stats (this correctly handles multiple hugepage sizes)
        self.collect_hugepage_per_numa_stats()

        # Sum from per-NUMA stats for accurate total (handles 2MB + 1GB etc.)
        self.hugepage_total_mb = 0.0
        self.hugepage_free_mb = 0.0
        for node_id, stats in self.hugepage_per_numa.items():
            self.hugepage_total_mb += stats["total_mb"]
            self.hugepage_free_mb += stats["free_mb"]

        self.hugepage_used_mb = self.hugepage_total_mb - self.hugepage_free_mb
        self.hugepage_used_history.append(self.hugepage_used_mb)

        if self.hugepage_used_mb > self.peak_hugepage_used_mb:
            self.peak_hugepage_used_mb = self.hugepage_used_mb

        # Fallback to /proc/meminfo if per-NUMA collection failed
        if self.hugepage_total_mb == 0 and self.hugepage_per_numa == {}:
            try:
                with open("/proc/meminfo") as f:
                    lines = f.read()
                huge_total = int(re.search(r"HugePages_Total:\s+(\d+)", lines).group(1))
                huge_free = int(re.search(r"HugePages_Free:\s+(\d+)", lines).group(1))
                huge_size = int(re.search(r"Hugepagesize:\s+(\d+)", lines).group(1))  # kB
                self.hugepage_total_mb = (huge_total * huge_size) / 1024
                self.hugepage_free_mb = (huge_free * huge_size) / 1024
                self.hugepage_used_mb = self.hugepage_total_mb - self.hugepage_free_mb
            except:
                pass

    def collect_hugepage_per_numa_stats(self):
        """Collect hugepage memory usage for each NUMA node"""
        self.hugepage_per_numa = {}
        try:
            node_dirs = [d for d in os.listdir("/sys/devices/system/node/") if d.startswith("node") and d[4:].isdigit()]
            for node in sorted(node_dirs, key=lambda x: int(x[4:])):
                node_id = int(node[4:])
                hugepages_dir = f"/sys/devices/system/node/{node}/hugepages"
                if not os.path.exists(hugepages_dir):
                    # Record the node even if hugepages directory doesn't exist (value is 0)
                    self.hugepage_per_numa[node_id] = {
                        "total_mb": 0.0,
                        "free_mb": 0.0,
                        "used_mb": 0.0,
                        "usage_pct": 0.0,
                    }
                    continue

                # Calculate MB for each hugepage size independently, then sum
                total_mb = 0.0
                free_mb = 0.0

                for subdir in os.listdir(hugepages_dir):
                    if subdir.startswith("hugepages-") and subdir.endswith("kB"):
                        size_match = re.search(r"hugepages-(\d+)kB", subdir)
                        if size_match:
                            huge_size_kb = int(size_match.group(1))
                            nr_path = os.path.join(hugepages_dir, subdir, "nr_hugepages")
                            free_path = os.path.join(hugepages_dir, subdir, "free_hugepages")
                            pages = 0
                            free_pages = 0
                            if os.path.exists(nr_path):
                                with open(nr_path) as f:
                                    pages = int(f.read().strip())
                            if os.path.exists(free_path):
                                with open(free_path) as f:
                                    free_pages = int(f.read().strip())
                            # Convert pages x size_kb to MB for this hugepage size
                            total_mb += (pages * huge_size_kb) / 1024
                            free_mb += (free_pages * huge_size_kb) / 1024

                used_mb = total_mb - free_mb
                self.hugepage_per_numa[node_id] = {
                    "total_mb": round(total_mb, 2),
                    "free_mb": round(free_mb, 2),
                    "used_mb": round(used_mb, 2),
                    "usage_pct": round(used_mb / total_mb * 100, 1) if total_mb > 0 else 0.0,
                }
        except Exception:
            pass

        self.hugepage_per_numa_history.append(
            {"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "nodes": dict(self.hugepage_per_numa)}
        )

    def print_numa_real_time(self):
        """Display the latest NUMA memory data from numa_memory_history.

        Uses the most recent entry from numa_memory_history (collected by
        collect_sample) rather than re-reading sysfs, to avoid duplicate
        data collection and keep history aligned with other metrics.
        """
        if not self.numa_memory_history:
            return
        latest = self.numa_memory_history[-1]["nodes"]
        focus_nodes = self.get_focus_numa_nodes()
        print("=" * 100)
        print("NUMA Node Memory Real-time Usage")
        for n in latest:
            nid = n["node"]
            sc = n.get("swap_cached_mb", 0)
            if nid in focus_nodes:
                # Focus NUMA: show detailed free/available/SwapCache
                avail = n.get("available_mb", 0)
                print(
                    f"    NUMA Node {nid:>2d} * | Total {n['total']:>8.2f} MB | Used {n['used']:>8.2f} MB | "
                    f"Free {n['free']:>8.2f} MB | Avail {avail:>8.2f} MB | SwapCache {sc:>6.1f} MB | Usage {n['usage']:>5.1f}%"
                )
            else:
                # Non-focus NUMA: show only basic total/used/SwapCache
                print(
                    f"    NUMA Node {nid:>2d}   | Total {n['total']:>8.2f} MB | Used {n['used']:>8.2f} MB | "
                    f"SwapCache {sc:>6.1f} MB | Usage {n['usage']:>5.1f}%"
                )
        print("=" * 100)

    def print_final_numa_stats(self):
        if not self.numa_memory_history:
            return

        summary = defaultdict(lambda: {"used": [], "usage": []})

        for entry in self.numa_memory_history:
            for n in entry["nodes"]:
                node_id = n["node"]

                summary[node_id]["used"].append(n["used"])
                summary[node_id]["usage"].append(n["usage"])

        print("\n[ NUMA Node Memory Statistics Summary ]")

        for node_id in sorted(summary.keys()):
            data = summary[node_id]
            avg_used = sum(data["used"]) / len(data["used"])
            max_used = max(data["used"])
            avg_usage = sum(data["usage"]) / len(data["usage"])
            max_usage = max(data["usage"])
            print(
                f"NUMA Node {node_id:>2d} | Avg Used {avg_used:>8.2f} MB | Peak {max_used:>8.2f} MB | Avg Usage {avg_usage:>5.1f}% | Peak Usage {max_usage:.1f}%"
            )

    # ===================== Get System NUMA Nodes =====================
    def get_available_numa_nodes(self):
        try:
            nodes = []
            for f in os.listdir("/sys/devices/system/node/"):
                if f.startswith("node") and f[4:].isdigit():
                    nodes.append(int(f[4:]))
            return sorted(nodes)
        except:
            return [0]

    def get_focus_numa_nodes(self) -> list:
        """Get focus NUMA nodes: CLI-specified target nodes + remote borrowing node.

        The remote borrowing node (NUMA5 by default — the platform's designated
        cross-socket borrowing node) is added for free-memory monitoring. If the
        configured remote node does not exist on this system, it is silently
        excluded. Returns only nodes that actually exist on the system.
        """
        focus = sorted(set(self.target_numa_nodes + ([self.remote_numa_id] if self.remote_numa_id is not None else [])))
        return [n for n in focus if n in self.available_numa_nodes]

    # ===================== Collect Specified NUMA Node CPU Usage =====================
    def collect_numa_cpu(self):
        try:
            all_cpu = psutil.cpu_percent(interval=None, percpu=True)
            node_to_cpus = self._numa_cpu_map_cached()

            for node in self.target_numa_nodes:
                cores = node_to_cpus.get(node, [])
                total = 0.0
                valid = 0
                for c in cores:
                    try:
                        total += all_cpu[c]
                        valid += 1
                    except IndexError:
                        pass
                avg = round(total / valid, 1) if valid > 0 else 0.0

                self.numa_cpu_history[node].append(avg)
                if avg > self.numa_cpu_peak[node]:
                    self.numa_cpu_peak[node] = avg
        except Exception:
            pass

    # ===================== Collect Host Machine Total CPU/Memory =====================
    def collect_host_stats(self):
        try:
            cpu = psutil.cpu_percent(interval=None)
            mem = psutil.virtual_memory()
            mem_used_mb = round(mem.used / 1024 / 1024, 2)
            mem_total_mb = round(mem.total / 1024 / 1024, 2)
            mem_usage = round(mem.percent, 1)
            self.host_cpu_history.append(cpu)
            self.host_mem_history.append(
                {
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "used_mb": mem_used_mb,
                    "total_mb": mem_total_mb,
                    "usage": mem_usage,
                }
            )
            if cpu > self.peak_host_cpu:
                self.peak_host_cpu = cpu
            if mem_used_mb > self.peak_host_mem_mb:
                self.peak_host_mem_mb = mem_used_mb
        except:
            pass

    # ===================== Collect Swap Usage =====================
    def collect_swap_stats(self, meminfo: dict | None = None, vmstat: dict | None = None):
        """Collect swap usage, swap cache, and swap in/out activity.

        ``meminfo`` / ``vmstat`` are the pre-read /proc/meminfo and /proc/vmstat
        dicts shared across one collect_sample cycle (swap_stats,
        host_mem_detail, host_pressure all consume them). When None (e.g. a
        standalone call), each is read here. Swap in/out rates divide by the
        ACTUAL monotonic elapsed window between samples (not the nominal
        ``self.interval``), so the MiB/s label holds even when the sampling
        cadence drifts from ``self.interval``. First sample (no prior
        baseline) records zero rates.
        """
        try:
            swap = psutil.swap_memory()
            swap_used_mb = round(swap.used / 1024 / 1024, 2)
            swap_total_mb = round(swap.total / 1024 / 1024, 2)
            swap_free_mb = round(swap.free / 1024 / 1024, 2)
            swap_usage = round(swap.percent, 1)

            # Swap Cache from /proc/meminfo (shared read -- deduped per cycle)
            if meminfo is None:
                meminfo = self._read_meminfo()
            swap_cached_mb = meminfo.get("SwapCached", 0)
            swap_cached_ratio = round(swap_cached_mb / swap_used_mb * 100, 1) if swap_used_mb > 0 else 0.0

            # Swap Activity from /proc/vmstat (shared read -- deduped per cycle)
            if vmstat is None:
                vmstat = self._read_vmstat()
            pswpin_now = vmstat.get("pswpin", 0)
            pswpout_now = vmstat.get("pswpout", 0)

            # Delta and rate: first sample has no prior baseline -> rate=0.
            # Divide by the ACTUAL monotonic elapsed window (not self.interval)
            # so the MiB/s rate is correct even when the sampling cadence
            # drifts (collection overhead, jitter). Falls back to self.interval
            # only when monotonic baseline is missing (first sample already
            # returns 0 via the _last_pswpin guard, so this branch is unused).
            now = time.monotonic()
            if self._last_pswpin is not None:
                pswpin_delta = pswpin_now - self._last_pswpin
                pswpout_delta = pswpout_now - self._last_pswpout
                elapsed = now - self._last_swap_monotonic if self._last_swap_monotonic is not None else self.interval
                if elapsed <= 0:
                    elapsed = self.interval if self.interval > 0 else 1.0
                # pswpin/pswpout are page counts (/proc/vmstat); convert to MiB/s
                # via the host page size so the rate matches its "MiB/s" label.
                mib_per_page = _PAGE_SIZE / 2**20
                swap_in_rate = round(pswpin_delta * mib_per_page / elapsed, 2)
                swap_out_rate = round(pswpout_delta * mib_per_page / elapsed, 2)
            else:
                pswpin_delta = 0
                pswpout_delta = 0
                swap_in_rate = 0
                swap_out_rate = 0
            self._last_pswpin = pswpin_now
            self._last_pswpout = pswpout_now
            self._last_swap_monotonic = now

            self.swap_history.append(
                {
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "capacity": {
                        "total_mb": swap_total_mb,
                        "free_mb": swap_free_mb,
                        "used_mb": swap_used_mb,
                        "usage_pct": swap_usage,
                    },
                    "cache": {
                        "cached_mb": swap_cached_mb,
                        "cached_ratio_pct": swap_cached_ratio,
                    },
                    "activity": {
                        "pswpin_delta": pswpin_delta,
                        "pswpout_delta": pswpout_delta,
                        "swap_in_rate": swap_in_rate,
                        "swap_out_rate": swap_out_rate,
                        "pswpin_cumulative": pswpin_now,
                        "pswpout_cumulative": pswpout_now,
                    },
                }
            )

            if swap_used_mb > self.peak_swap_used_mb:
                self.peak_swap_used_mb = swap_used_mb
            if swap_cached_mb > self.peak_swap_cached_mb:
                self.peak_swap_cached_mb = swap_cached_mb
        except:
            pass

    # ===================== Collect Disk I/O =====================
    def collect_disk_stats(self):
        """Collect per-device disk I/O rates from /sys/block/<dev>/stat.

        Reads sectors read/written, inflight, busy-ms, I/O counts, I/O time,
        and weighted I/O time for each device in self.target_disks, then
        computes per-second rates + avg queue depth + per-I/O await latency
        via deltas from the previous sample normalized by the ACTUAL elapsed
        wall-clock time between reads (time.monotonic delta), not the nominal
        self.interval. Disk is sub-sampled at a 1-second cadence by
        _disk_subsample_sleep regardless of the general sampling interval, so
        r_mb_s / w_mb_s reflect a true 1-second window -- burst peaks are not
        smeared over a longer interval. First sample (no prior baseline)
        records zero rates. Missing devices are skipped silently. Bus (nvme vs
        sata) is classified by device name for downstream grouping; it is not
        an AgentEnv-specific signal.
        """
        cur_stats = {}
        raw_fields: dict[str, list[str]] = {}  # dev -> raw /sys/block/<dev>/stat splits
        for dev in self.target_disks:
            try:
                with open(f"/sys/block/{dev}/stat") as f:
                    fields = f.read().split()
                if len(fields) < 11:
                    continue
                raw_fields[dev] = fields  # keep ALL fields for the raw dump
                cur_stats[dev] = {
                    "sectors_read": int(fields[2]),
                    "sectors_written": int(fields[6]),
                    "inflight": int(fields[8]),
                    "ms_io": int(fields[9]),
                    "reads_completed": int(fields[0]),
                    "writes_completed": int(fields[4]),
                    "read_ms": int(fields[3]),
                    "write_ms": int(fields[7]),
                    "weighted_ms": int(fields[10]),
                    "bus": "nvme" if dev.startswith("nvme") else "sata",
                }
            except (OSError, ValueError):
                continue

        # Divide by the real time between reads, not self.interval. The disk
        # sub-sampler ticks at 1s, so this is a true per-second rate even when
        # the general sampling interval is larger (3s/5s/...).
        now = time.monotonic()
        prev = self._last_diskstats
        if prev is None:
            elapsed = 0.0
        else:
            elapsed = now - (self._last_disk_monotonic if self._last_disk_monotonic is not None else now)
        self._last_disk_monotonic = now
        rates = _compute_disk_io_rates(cur_stats, prev, elapsed)
        self._last_diskstats = cur_stats

        peak_write = 0.0
        for dev, r in rates.items():
            if r["w_mb_s"] > peak_write:
                peak_write = r["w_mb_s"]
        if peak_write > self.peak_disk_write_mb_s:
            self.peak_disk_write_mb_s = peak_write

        self.disk_history.append({"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "disks": rates})

        # Raw /sys/block/<dev>/stat counters -> disk_io_raw.csv (1s cadence, same
        # background writer; enqueue is O(1) so the sub-sampler never blocks).
        if self._proc_raw is not None and raw_fields:
            self._proc_raw.enqueue_disk(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), raw_fields)

    # ===================== Collect Host Memory Detail =====================
    def collect_host_mem_detail(self, meminfo: dict | None = None):
        """Collect host memory breakdown (cached/buffers/dirty/writeback) from /proc/meminfo.

        ``meminfo`` is the pre-read /proc/meminfo dict shared across one
        collect_sample cycle (also consumed by collect_swap_stats and
        collect_host_pressure); read here when None. cached = Cached +
        SReclaimable (page cache + reclaimable slab). All values in MB.
        """
        try:
            if meminfo is None:
                meminfo = self._read_meminfo()
            mi = meminfo
            cached_mb = round(mi.get("Cached", 0) + mi.get("SReclaimable", 0), 2)
            buffers_mb = round(mi.get("Buffers", 0), 2)
            dirty_mb = round(mi.get("Dirty", 0), 2)
            writeback_mb = round(mi.get("Writeback", 0), 2)
            self.host_mem_detail_history.append(
                {
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "cached_mb": cached_mb,
                    "buffers_mb": buffers_mb,
                    "dirty_mb": dirty_mb,
                    "writeback_mb": writeback_mb,
                }
            )
            if dirty_mb > self.peak_dirty_mb:
                self.peak_dirty_mb = dirty_mb
            if writeback_mb > self.peak_writeback_mb:
                self.peak_writeback_mb = writeback_mb
        except Exception:
            pass

    # ===================== Collect ublk Device Count =====================
    def collect_ublk_count(self):
        """Count /dev/ublkb* devices (userspace block devices).

        Generic block-device inventory; not tied to any sandbox runtime.
        """
        try:
            n = len(glob.glob("/dev/ublkb*"))
            self.ublk_history.append({"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "ublk_devices": n})
            if n > self.peak_ublk_devices:
                self.peak_ublk_devices = n
        except Exception:
            pass

    @staticmethod
    def _find_pid_by_comm(comm: str, cmdline_substr: str | None = None) -> int | None:
        """First pid whose /proc/<pid>/comm equals ``comm`` (one /proc scan).

        None when no such process exists (e.g. the ublk daemon is absent on a
        non-ublk host). Used by collect_ublk_daemon to resolve the daemon pid
        lazily; rescans on miss so a daemon started after the monitor launches
        is picked up on a later sample.

        ``cmdline_substr`` (optional) additionally requires the substring in
        /proc/<pid>/cmdline -- necessary for generic comms like ``server``
        (the agentENV server's comm is ``server``; without a filter any process
        literally named ``server`` would match). The cmdline file is
        NUL-joined; the match is a plain substring over those raw bytes (parity
        with spotbox ``find_pid_by_comm``), so ``release/server`` also matches
        ``release/server-foo`` -- accept the minor over-match and use a more
        specific substr when it matters. Returns the FIRST match and short-
        circuits; collect_host_procs uses _resolve_hostproc_pid when it also
        needs a multi-match count for the "spec too loose" WARNING.
        """
        for path in glob.glob("/proc/[0-9]*/comm"):
            try:
                with open(path) as f:
                    if f.read().strip() != comm:
                        continue
                pid = int(path.split("/")[2])
                if cmdline_substr is not None:
                    try:
                        with open(f"/proc/{pid}/cmdline") as cf:
                            if cmdline_substr not in cf.read():
                                continue
                    except OSError:
                        continue
                return pid
            except (OSError, ValueError):
                pass
        return None

    @staticmethod
    def _resolve_hostproc_pid(comm: str, cmdline_substr: str | None) -> tuple[int | None, int]:
        """Resolve a host-proc spec to (first_pid, match_count_up_to_2).

        One /proc scan collecting up to 2 matches so the caller can emit a
        one-time "spec matches N pids -- tracking the first" WARNING when a
        spec is too loose (e.g. ``server`` with no cmdline filter). Stops at 2
        (the count only needs to distinguish 1 vs many). Used by
        collect_host_procs at (re)resolve time only -- never per-sample -- so
        the full /proc scan cost (~100ms) is paid once per spec per restart,
        not every tick.
        """
        matches: list[int] = []
        for path in glob.glob("/proc/[0-9]*/comm"):
            try:
                with open(path) as f:
                    if f.read().strip() != comm:
                        continue
                pid = int(path.split("/")[2])
                if cmdline_substr is not None:
                    try:
                        with open(f"/proc/{pid}/cmdline") as cf:
                            if cmdline_substr not in cf.read():
                                continue
                    except OSError:
                        continue
                matches.append(pid)
                if len(matches) >= 2:
                    break
            except (OSError, ValueError):
                pass
        return (matches[0] if matches else None, len(matches))

    # ===================== Collect ublk Daemon CPU =====================
    def collect_ublk_daemon(self):
        """CPU cores consumed by the uvm-ublk-daemon process.

        Resolves the daemon pid lazily on first call, then reads
        /proc/<pid>/stat utime+stime each sample and converts the jiffy delta
        to cores (= CPU-seconds / wall-seconds). On hosts without a ublk
        runtime the daemon is absent -> history stays empty -> the
        host_resources.xlsx ublk_cores column pads to 0 (graceful degrade).
        """
        if self._ublk_daemon_pid is None:
            self._ublk_daemon_pid = self._find_pid_by_comm(_UBLK_DAEMON_COMM)
        pid = self._ublk_daemon_pid
        if not pid:
            return
        js = self._read_pid_jiffies(pid)
        if js is None:
            # daemon exited/restarted; re-resolve next sample
            self._ublk_daemon_pid = None
            self._prev_ublk_daemon_jiffies = None
            return
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        # Raw cumulative jiffies for ublk_cpu_raw.csv; agent derives CPU% via
        # delta -- the derived 'cores' below alone is lossy (rounded, rate-only).
        if self._proc_raw is not None:
            self._proc_raw.enqueue_ublk(ts, pid, js[0], js[1])
        jiffies = js[0] + js[1]  # utime + stime
        now = time.perf_counter()
        cores = 0.0
        if self._prev_ublk_daemon_jiffies is not None:
            dt = max(1e-6, now - self._ublk_daemon_last_t)
            cores = round(max(0, jiffies - self._prev_ublk_daemon_jiffies) / _CLK_TCK / dt, 3)
        self._prev_ublk_daemon_jiffies = jiffies
        self._ublk_daemon_last_t = now
        self.ublk_daemon_history.append({"ts": ts, "cores": cores})

    # ===================== Collect user-specified host processes ============
    def set_host_proc_specs(self, specs: list[str]) -> None:
        """Parse + store ``--host-proc`` specs for collect_host_procs.

        Each spec is ``comm`` or ``comm:cmdline_substr``. The label used in
        the raw CSV / sheet is the comm (a pid column disambiguates restarts).
        A comm without a cmdline filter is allowed but loose (any process with
        that comm matches); ``server`` should carry ``:release/server`` etc.
        Commas are NOT valid inside a spec (a comma-separated YAML value would
        split a spec across the comma); use a YAML list for cmdline filters
        that need a comma.
        """
        parsed: list[tuple[str, str, str | None]] = []
        for spec in specs:
            spec = spec.strip()
            if not spec:
                continue
            if ":" in spec:
                comm, substr = spec.split(":", 1)
                comm, substr = comm.strip(), substr.strip()
                if not comm:
                    logger.warning("host-proc spec %r has empty comm; dropped", spec)
                    continue
                parsed.append((comm, comm, substr or None))
            else:
                parsed.append((spec, spec, None))
        self._hostproc_specs = parsed
        if parsed:
            labels = [f"{c}:{s}" if s else c for _, c, s in parsed]
            print(f"[INFO] host-proc specs: {labels}")

    # Host-proc "not found" consecutive-miss threshold before a WARNING fires
    # (at 1s sub-sample cadence, ~5s of "never seen this spec" -> likely a typo).
    _HOSTPROC_MISS_THRESHOLD = 5

