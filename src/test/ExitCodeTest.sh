#!/bin/sh
# Test for the exit code after a fatal error in a worker thread
# Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)
#
# This file is part of OpenLogReplicator, licensed under the GNU Affero General Public License version 3 or later;
# see the file LICENSE.
#
# A replicator, reader or writer thread which fails logs the error and stops the process with Ctx::stopHard(). The
# process has to end with a non-zero exit code then, or a supervisor (systemd, Kubernetes) takes the stop for a normal
# end and neither restarts nor alerts. The batch replicator below fails at once because its checkpoint directory does
# not exist; no database is needed.
#
# Usage: ExitCodeTest.sh <path to the OpenLogReplicator binary>

binary="$1"
dir=$(mktemp -d) || exit 2
trap 'rm -rf "$dir"' EXIT

cat > "$dir/config.json" <<'JSON'
{
  "version": "2.0.0",
  "state": {"type": "disk", "path": "missing-state-dir"},
  "source": [{
    "alias": "S1", "name": "TEST",
    "reader": {"type": "batch", "redo-log": ["missing.arc"], "log-archive-format": "", "start-scn": 1},
    "format": {"type": "json"},
    "flags": 2
  }],
  "target": [{"alias": "T1", "source": "S1", "writer": {"type": "file", "output": "output.json"}}]
}
JSON

cd "$dir" || exit 2
timeout 60 "$binary" -f config.json > olr.log 2>&1
code=$?
cat olr.log

failures=0
if grep -q " ERROR " olr.log; then
    echo "ok   the replicator thread logged an error"
else
    echo "FAIL the replicator thread logged no error"
    failures=$((failures + 1))
fi
if [ "$code" -eq 124 ]; then
    echo "FAIL the process did not end within 60 s after the error"
    failures=$((failures + 1))
elif [ "$code" -ne 0 ]; then
    echo "ok   exit code after the error is $code"
else
    echo "FAIL exit code after the error is 0"
    failures=$((failures + 1))
fi

if [ "$failures" -gt 0 ]; then
    echo "$failures check(s) failed"
    exit 1
fi
echo "all checks passed"
exit 0
