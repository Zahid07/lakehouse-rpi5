ClickHouse run ch_run1: 6 producers added every 30 min (19:54 -> 22:24 UTC, 2026-10-07), all ending 01:54 UTC 2026-10-08.
51,300,150 readings, 0 missing. sysmon 19:44 -> 02:14 UTC (10 min idle baseline first).
throttled = 0x50000 throughout = sticky "since boot" bits set BEFORE the run; no live throttling during it. Max temp 80.4 C.
Per-reading audit (v_audit_reading) expired (24 h raw retention) before export. Latency here is per machine-minute:
last_reading_e2e_ms = the minute's last reading, sent -> stored (one sample per machine-minute). Includes the laptop clock offset;
min_transport_ms estimates that offset.
