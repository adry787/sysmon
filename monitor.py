"""SysMon - Server monitoring agent."""

import os
import sys
import json
import time
import socket
import sqlite3
import logging
import platform
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class SystemMetrics:
    timestamp: float
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    memory_used_mb: float = 0.0
    memory_total_mb: float = 0.0
    disk_percent: float = 0.0
    disk_used_gb: float = 0.0
    disk_total_gb: float = 0.0
    net_sent_bytes: int = 0
    net_recv_bytes: int = 0
    load_avg: tuple = (0.0, 0.0, 0.0)
    uptime_seconds: int = 0
    process_count: int = 0

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "time": datetime.fromtimestamp(self.timestamp).isoformat(),
            "cpu_percent": self.cpu_percent,
            "memory": {
                "percent": self.memory_percent,
                "used_mb": round(self.memory_used_mb, 1),
                "total_mb": round(self.memory_total_mb, 1),
            },
            "disk": {
                "percent": self.disk_percent,
                "used_gb": round(self.disk_used_gb, 1),
                "total_gb": round(self.disk_total_gb, 1),
            },
            "network": {
                "sent_bytes": self.net_sent_bytes,
                "recv_bytes": self.net_recv_bytes,
            },
            "load_avg": list(self.load_avg),
            "uptime_seconds": self.uptime_seconds,
            "process_count": self.process_count,
        }


@dataclass
class AlertRule:
    name: str
    metric: str
    threshold: float
    condition: str  # "gt", "lt", "eq"
    cooldown: int = 300
    last_triggered: float = 0.0
    enabled: bool = True

    def check(self, value: float) -> bool:
        if not self.enabled:
            return False
        now = time.time()
        if now - self.last_triggered < self.cooldown:
            return False

        triggered = False
        if self.condition == "gt" and value > self.threshold:
            triggered = True
        elif self.condition == "lt" and value < self.threshold:
            triggered = True
        elif self.condition == "eq" and abs(value - self.threshold) < 0.01:
            triggered = True

        if triggered:
            self.last_triggered = now
        return triggered


class MetricsCollector:
    """Collect system metrics from the operating system."""

    def __init__(self):
        self._prev_net_sent = 0
        self._prev_net_recv = 0
        self._prev_net_time = 0.0

    def collect(self) -> SystemMetrics:
        metrics = SystemMetrics(timestamp=time.time())

        metrics.cpu_percent = self._get_cpu_percent()
        mem = self._get_memory()
        metrics.memory_percent = mem["percent"]
        metrics.memory_used_mb = mem["used_mb"]
        metrics.memory_total_mb = mem["total_mb"]
        disk = self._get_disk("/")
        metrics.disk_percent = disk["percent"]
        metrics.disk_used_gb = disk["used_gb"]
        metrics.disk_total_gb = disk["total_gb"]
        net = self._get_network()
        metrics.net_sent_bytes = net["sent"]
        metrics.net_recv_bytes = net["recv"]
        metrics.load_avg = self._get_load_avg()
        metrics.uptime_seconds = self._get_uptime()
        metrics.process_count = self._get_process_count()

        return metrics

    def _get_cpu_percent(self) -> float:
        try:
            with open("/proc/stat") as f:
                line = f.readline()
            parts = line.split()
            idle = float(parts[4])
            total = sum(float(p) for p in parts[1:])
            return round(100 * (1 - idle / max(total, 1)), 1)
        except Exception:
            return 0.0

    def _get_memory(self) -> dict:
        try:
            with open("/proc/meminfo") as f:
                info = {}
                for line in f:
                    parts = line.split()
                    key = parts[0].rstrip(":")
                    info[key] = int(parts[1])
            total = info.get("MemTotal", 1) / 1024
            available = info.get("MemAvailable", 0) / 1024
            used = total - available
            return {
                "percent": round(100 * used / max(total, 1), 1),
                "used_mb": used,
                "total_mb": total,
            }
        except Exception:
            return {"percent": 0, "used_mb": 0, "total_mb": 0}

    def _get_disk(self, path: str = "/") -> dict:
        try:
            stat = os.statvfs(path)
            total = stat.f_blocks * stat.f_frsize / (1024 ** 3)
            free = stat.f_bavail * stat.f_frsize / (1024 ** 3)
            used = total - free
            return {
                "percent": round(100 * used / max(total, 1), 1),
                "used_gb": used,
                "total_gb": total,
            }
        except Exception:
            return {"percent": 0, "used_gb": 0, "total_gb": 0}

    def _get_network(self) -> dict:
        try:
            with open("/proc/net/dev") as f:
                lines = f.readlines()[2:]
            sent, recv = 0, 0
            for line in lines:
                parts = line.split()
                if len(parts) >= 10:
                    recv += int(parts[1])
                    sent += int(parts[9])
            return {"sent": sent, "recv": recv}
        except Exception:
            return {"sent": 0, "recv": 0}

    def _get_load_avg(self) -> tuple:
        try:
            with open("/proc/loadavg") as f:
                parts = f.read().split()
            return (float(parts[0]), float(parts[1]), float(parts[2]))
        except Exception:
            return (0.0, 0.0, 0.0)

    def _get_uptime(self) -> int:
        try:
            with open("/proc/uptime") as f:
                return int(float(f.read().split()[0]))
        except Exception:
            return 0

    def _get_process_count(self) -> int:
        try:
            return len(os.listdir("/proc"))
        except Exception:
            return 0


class MetricsStore:
    """SQLite storage for historical metrics."""

    def __init__(self, db_path: str = "sysmon.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL,
                    cpu_percent REAL,
                    memory_percent REAL,
                    disk_percent REAL,
                    data_json TEXT
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_timestamp ON metrics(timestamp)
            """)

    def insert(self, metrics: SystemMetrics) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO metrics (timestamp, cpu_percent, memory_percent, disk_percent, data_json) VALUES (?, ?, ?, ?, ?)",
                (metrics.timestamp, metrics.cpu_percent, metrics.memory_percent,
                 metrics.disk_percent, json.dumps(metrics.to_dict())),
            )

    def query(self, start: float = None, end: float = None, limit: int = 100) -> list[dict]:
        with sqlite3.connect(self.db_path) as conn:
            query = "SELECT data_json FROM metrics WHERE 1=1"
            params = []
            if start:
                query += " AND timestamp >= ?"
                params.append(start)
            if end:
                query += " AND timestamp <= ?"
                params.append(end)
            query += " ORDER BY timestamp DESC LIMIT ?"
            params.append(limit)
            rows = conn.execute(query, params).fetchall()
        return [json.loads(row[0]) for row in rows]


class AlertManager:
    """Manage and evaluate alert rules."""

    def __init__(self):
        self.rules: list[AlertRule] = []
        self.alert_history: list[dict] = []

    def add_rule(self, rule: AlertRule) -> None:
        self.rules.append(rule)

    def evaluate(self, metrics: SystemMetrics) -> list[dict]:
        alerts = []
        metric_map = {
            "cpu_percent": metrics.cpu_percent,
            "memory_percent": metrics.memory_percent,
            "disk_percent": metrics.disk_percent,
        }
        for rule in self.rules:
            value = metric_map.get(rule.metric, 0)
            if rule.check(value):
                alert = {
                    "rule": rule.name,
                    "metric": rule.metric,
                    "value": value,
                    "threshold": rule.threshold,
                    "condition": rule.condition,
                    "timestamp": time.time(),
                }
                alerts.append(alert)
                self.alert_history.append(alert)
                logger.warning("ALERT: %s = %.1f (threshold: %.1f %s)",
                               rule.name, value, rule.threshold, rule.condition)
        return alerts


class SysMon:
    """Main monitoring agent."""

    def __init__(self, db_path: str = "sysmon.db"):
        self.collector = MetricsCollector()
        self.store = MetricsStore(db_path)
        self.alert_manager = AlertManager()
        self.running = False

        self.alert_manager.add_rule(AlertRule(
            name="High CPU", metric="cpu_percent", threshold=90, condition="gt", cooldown=60
        ))
        self.alert_manager.add_rule(AlertRule(
            name="High Memory", metric="memory_percent", threshold=90, condition="gt", cooldown=60
        ))
        self.alert_manager.add_rule(AlertRule(
            name="Disk Full", metric="disk_percent", threshold=95, condition="gt", cooldown=300
        ))

    def collect_and_store(self) -> SystemMetrics:
        metrics = self.collector.collect()
        self.store.insert(metrics)
        alerts = self.alert_manager.evaluate(metrics)
        if alerts:
            logger.info("Generated %d alerts", len(alerts))
        return metrics

    def run(self, interval: int = 5) -> None:
        self.running = True
        logger.info("SysMon started (interval=%ds)", interval)
        try:
            while self.running:
                metrics = self.collect_and_store()
                logger.info("CPU: %.1f%% | MEM: %.1f%% | DISK: %.1f%% | Procs: %d",
                            metrics.cpu_percent, metrics.memory_percent,
                            metrics.disk_percent, metrics.process_count)
                time.sleep(interval)
        except KeyboardInterrupt:
            self.running = False
            logger.info("SysMon stopped")


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="SysMon - Server Monitor")
    parser.add_argument("--interval", type=int, default=5, help="Collection interval (seconds)")
    parser.add_argument("--once", action="store_true", help="Collect once and exit")
    parser.add_argument("--history", type=int, help="Show last N readings")
    args = parser.parse_args()

    monitor = SysMon()

    if args.history:
        data = monitor.store.query(limit=args.history)
        for d in reversed(data):
            print(f"{d['time']} | CPU: {d['cpu_percent']:.1f}% | MEM: {d['memory']['percent']:.1f}%")
    elif args.once:
        m = monitor.collect_and_store()
        print(json.dumps(m.to_dict(), indent=2))
    else:
        monitor.run(interval=args.interval)
