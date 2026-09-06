#!/usr/bin/env bash
# One soak cycle: land a drop, drain it, sample the trend. Driven by cron.
#
# Cron is the deployment shape PLAN.md describes and CONTEXT.md 1.6 forces: while
# one process holds a DuckDB file nothing else can open it, even read-only, so
# the engine opens, drains and exits rather than living as a daemon. The soak
# should look like the thing it is soaking.
#
# Install (every minute):
#   crontab -e
#   * * * * * /home/zahid/python_scripts/lakehouse-rpi5/tools/soak/cycle.sh
#
# Stop:  crontab -e and delete the line. Nothing else is left running.

set -uo pipefail

REPO="${REPO:-/home/zahid/python_scripts/lakehouse-rpi5}"
export DUCKSTREAM_SOAK="${DUCKSTREAM_SOAK:-/home/zahid/duckstream-soak}"
PY="$REPO/.venv/bin/python"
LOG="$DUCKSTREAM_SOAK/soak.log"
TREND="$DUCKSTREAM_SOAK/trend.csv"

mkdir -p "$DUCKSTREAM_SOAK/landing"

# `cd` into the repo, not because the tools need it, but because DuckDB's
# `temp_directory` default is `.tmp` **relative to the CWD** (CONTEXT.md 1.24).
# The config disables spilling outright, so this only matters if somebody edits
# that setting out -- in which case a predictable CWD beats whatever directory
# cron happened to start in.
cd "$REPO" || exit 1

stamp() { date -u +%Y-%m-%dT%H:%M:%SZ; }

{
  echo "--- $(stamp) cycle start"

  # 1. Land one drop. Every 20th is stamped behind the stream, which is what
  #    drives the watermark, the lateness horizon and eventually a seal.
  "$PY" tools/soak/feed.py \
      --landing "$DUCKSTREAM_SOAK/landing" \
      --rows 600 --late-every 20 2>&1

  # 2. Drain. Non-zero is informative rather than fatal: `duckstream run` exits
  #    non-zero for failing, halted, backed-off *and* quarantined models, and a
  #    quarantine is exactly the event a soak is meant to survive and record.
  #    So log the code and keep the loop alive -- a soak that stops at the first
  #    unhealthy tick measures nothing about running unattended.
  "$PY" -m duckstream run --config tools/soak/soak.yaml 2>&1
  code=$?
  echo "duckstream run exit=$code"

  # 3. Sample the trend, every cycle, because the shape over time is the result.
  "$PY" tools/soak/check.py --csv "$TREND" 2>&1 | tail -4

  echo "--- $(stamp) cycle end"
} >> "$LOG" 2>&1

# Keep the log from being the thing that fills the card. 20 MB is days of this.
if [ -f "$LOG" ] && [ "$(stat -c%s "$LOG")" -gt 20971520 ]; then
  tail -c 5242880 "$LOG" > "$LOG.trim" && mv "$LOG.trim" "$LOG"
fi
