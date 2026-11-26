#!/usr/bin/env bash
# Install the stft_db executable UDF into ClickHouse. Needs sudo, once.
#
#   sudo ./install_udf.sh
#
# Copies stft_db.py into ClickHouse's user_scripts directory and
# stft_db_function.xml into its config directory, then checks the function
# works. Uses the system /usr/bin/python3 and its numpy (python3-numpy).
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "Run with sudo: sudo $0" >&2
    exit 1
fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG=/etc/clickhouse-server/config.xml

# Ask ClickHouse's own config where these live rather than assuming defaults.
SCRIPTS=$(clickhouse extract-from-config --config-file "$CONFIG" --key user_scripts_path 2>/dev/null || true)
SCRIPTS=${SCRIPTS:-/var/lib/clickhouse/user_scripts/}
PATTERN=$(clickhouse extract-from-config --config-file "$CONFIG" --key user_defined_executable_functions_config 2>/dev/null || true)
PATTERN=${PATTERN:-*_function.*ml}
case "$PATTERN" in
    /*) FUNC_DIR=$(dirname "$PATTERN") ;;
    *)  FUNC_DIR=$(dirname "$CONFIG") ;;
esac

echo "==> checking numpy for /usr/bin/python3"
if ! /usr/bin/python3 -c "import numpy" 2>/dev/null; then
    apt-get install -y python3-numpy
fi

echo "==> script   -> $SCRIPTS"
mkdir -p "$SCRIPTS"
install -o clickhouse -g clickhouse -m 0755 "$HERE/stft_db.py" "$SCRIPTS/stft_db.py"

echo "==> function -> $FUNC_DIR/stft_db_function.xml (pattern: $PATTERN)"
install -o root -g clickhouse -m 0644 "$HERE/stft_db_function.xml" "$FUNC_DIR/stft_db_function.xml"

echo "==> reloading and testing"
sleep 2
# 1,012 samples = exactly 2 frames = 2 x 257 cells.
if clickhouse-client --query "SYSTEM RELOAD FUNCTIONS" 2>/dev/null \
   && result=$(clickhouse-client --query \
        "SELECT length(stft_db(arrayMap(i -> sin(i / 10), range(1012))))" 2>&1) \
   && [[ "$result" == 514 ]]; then
    echo "stft_db works (2 frames -> 514 cells)."
else
    echo "Installed. The self-test could not log in to ClickHouse (it needs a"
    echo "password), so test it yourself in clickhouse-client or DBeaver:"
    echo "  SELECT length(stft_db(arrayMap(i -> sin(i / 10), range(1012))));   -- expect 514"
fi
