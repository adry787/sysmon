# SysMon 🖥️

Lightweight server monitoring agent with real-time metrics, alerting, and historical data storage. Monitors CPU, memory, disk, network, and custom services.

## Features

- CPU, memory, disk, network monitoring
- Service health checks (HTTP, TCP, process)
- Custom metric collection
- Alert via webhook/Telegram/email
- SQLite historical data
- REST API for dashboards

## Setup

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python monitor.py --interval 5
```

## GPU Requirements

| Component | GPU | VRAM | Notes |
|-----------|-----|------|-------|
| Monitoring agent | None | — | Minimal CPU usage |

## Supported Platforms

- Linux (full support)
- macOS (full support)
- Windows (partial, no process monitoring)
