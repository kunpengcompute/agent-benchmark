#!/usr/bin/env bash
# agent-browser shim -- transparent environment hardening for trace replay.
# Infrastructure races that are NOT part of the agent's workload:
#   1. CDP cold start: first launch can fail with "CDP WebSocket connect failed".
#   2. GitLab pages never fire 'load' (readyState stuck at 'interactive') though
#      the DOM is complete -> fall back to a domcontentloaded / readyState wait.
#   3. net::ERR_ABORTED when a navigation interrupts the previous page's
#      in-flight requests -> retry the navigation.
#   4. Element not found on a freshly-opened SPA page -> retry with backoff.
REAL=/usr/local/node24/bin/agent-browser

is_load_timeout() { case "$1" in *"Timed out after"*"waiting for load state"*) return 0;; *) return 1;; esac; }
is_cdp_coldstart() { case "$1" in *"CDP WebSocket connect failed"*|*"Auto-launch failed"*|*"Resource temporarily unavailable"*|*"Failed to read"*) return 0;; *) return 1;; esac; }
is_aborted() { case "$1" in *"ERR_ABORTED"*) return 0;; *) return 1;; esac; }
is_nav() { case "$1" in open|goto|navigate) return 0;; *) return 1;; esac; }
is_find() { case "$1" in find|findfirst|select|fill|click) return 0;; *) return 1;; esac; }
is_element_not_found() { case "$1" in *"No element at index 0"*|*"No element found"*|*"Element not found"*) return 0;; *) return 1;; esac; }
is_daemon_crash() { [ -z "$1" ]; }

FAILMARK=$'\xe2\x9c\x97'   # U+2717 BALLOT X (agent-browser failure prefix)

run_real() {
  out=$("$REAL" "$@" 2>&1); rc=$?
  # agent-browser 0.43.0 returns exit code 2 for successful operations
  # (0.41.2 returned 0). Normalize: success marker ✓ = rc 0, fail marker ✗ = rc 1.
  case "$out" in *"$FAILMARK"*) rc=1;; *$'\xe2\x9c\x93'*) rc=0;; esac
}

# Page is usable if readyState is at least 'interactive' and a body exists.
page_ready() {
  local js='(() => { try { return (document.readyState !== "loading" && !!document.body) ? "READY" : "NOTREADY"; } catch (e) { return "NOTREADY"; } })()'
  local b64 out2
  b64=$(printf '%s' "$js" | base64 -w0)
  out2=$("$REAL" eval -b "$b64" 2>/dev/null) || return 1
  case "$out2" in *READY*) return 0;; *) return 1;; esac
}

run_real "$@"

# 1) cold-start: retry with backoff (browser process needs time to bind CDP)
if [ $rc -ne 0 ] && is_cdp_coldstart "$out"; then
  for d in 1 2 4 8 12; do
    sleep $d
    run_real "$@"
    [ $rc -eq 0 ] && break
    is_cdp_coldstart "$out" || break
  done
fi

# 3) aborted navigation: settle briefly and retry
if [ $rc -ne 0 ] && is_nav "$1" && is_aborted "$out"; then
  for d in 1 2 3; do
    sleep $d
    run_real "$@"
    [ $rc -eq 0 ] && break
    is_aborted "$out" || break
  done
fi

# 2) load-state timeout on a page that is nonetheless rendered.
#    The DOM is what the agent acts on; a hanging sub-resource must not fail the step.
if [ $rc -ne 0 ] && is_nav "$1" && is_load_timeout "$out"; then
  for d in 1 2 3; do
    if "$REAL" wait --load domcontentloaded >/dev/null 2>&1; then echo "Done (fallback)"; sleep 3; exit 0; fi
    if page_ready; then echo "Done (fallback)"; sleep 3; exit 0; fi
    sleep $d
  done
  # last resort: the navigation itself did happen; report the page we are on
  if page_ready; then echo "Done (fallback)"; sleep 3; exit 0; fi
fi

# 4) element not found on a freshly-opened SPA page: retry with backoff.
#    Vue.js / React components may render after the load event.
if [ $rc -ne 0 ] && is_find "$1" && is_element_not_found "$out"; then
  for d in 5 10 15; do
    sleep $d
    run_real "$@"
    [ $rc -eq 0 ] && break
    is_element_not_found "$out" || break
  done
fi

# 5) daemon crash: empty output with non-zero exit means the agent-browser
#    daemon died (OOM, signal, server overload). Restart it with backoff.
if [ $rc -ne 0 ] && is_daemon_crash "$out"; then
  for d in 2 5 10; do
    sleep $d
    run_real "$@"
    [ $rc -eq 0 ] && break
    is_daemon_crash "$out" || break
  done
fi

printf '%s\n' "$out"
case "$out" in *"$FAILMARK"*) exit 1;; esac
exit $rc
