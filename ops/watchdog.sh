#!/usr/bin/env bash
# Deployed manually to a separately-operated Good Jobs instance's host — not the
# CI/CD-managed server in README's Deployment section. This copy is for version
# control and review; editing it here does not update the live copy. To ship a
# change, copy this file to wherever that host's cron entry points (`crontab -l`
# there), `chmod +x` it, and confirm the schedule still matches (`*/5 * * * *`
# as of this writing).

# GoodJobs watchdog — catches failures the log-grep restart rules miss:
# container-down states and host DNS breakage (e.g. Tailscale accept-dns
# flipping back on and killing name resolution, which silently crash-looped
# the backend on 2026-07-12 without matching any "scrape timeout" /
# "all sources timed out" / "batch API failed" pattern).

CONTAINER="goodjobs-backend-1"
LOG="/var/log/goodjobs-restart.log"
STATE="/var/log/goodjobs-watchdog-restart-count"
DNS_CHECK_HOST="api.cloudflare.com"

log() { echo "$(date '+%a %b %d %T %z %Y'): $1" >> "$LOG"; }

# 1. Host DNS check — root cause of the last incident.
if ! getent hosts "$DNS_CHECK_HOST" > /dev/null 2>&1; then
  log "DNS resolution broken (cannot resolve $DNS_CHECK_HOST) — reapplying DNS fix"
  command -v tailscale > /dev/null 2>&1 && tailscale set --accept-dns=false
  mkdir -p /etc/systemd/resolved.conf.d
  printf "[Resolve]\nDNS=1.1.1.1 8.8.8.8\n" > /etc/systemd/resolved.conf.d/fallback.conf
  systemctl restart systemd-resolved
  sleep 2
  if getent hosts "$DNS_CHECK_HOST" > /dev/null 2>&1; then
    log "DNS resolution restored"
  else
    log "DNS resolution STILL broken after fix attempt — manual intervention needed"
  fi
fi

# 2. Container health check — catches startup crash-loops regardless of
#    what the backend logs actually say.
status=$(docker inspect -f '{{.State.Status}}' "$CONTAINER" 2>/dev/null)
restart_count=$(docker inspect -f '{{.RestartCount}}' "$CONTAINER" 2>/dev/null)
last_count=$(cat "$STATE" 2>/dev/null || echo 0)

if [ -z "$status" ]; then
  log "container $CONTAINER not found"
elif [ "$status" != "running" ]; then
  log "container status is '$status' (not running) — restarting"
  docker restart "$CONTAINER"
elif [ -n "$restart_count" ] && [ "$restart_count" -gt "$last_count" ]; then
  delta=$((restart_count - last_count))
  if [ "$delta" -ge 3 ]; then
    log "container restarted $delta times since last check (crash-loop detected, status=$status)"
  fi
fi

[ -n "$restart_count" ] && echo "$restart_count" > "$STATE"

# 3. Application-level liveness check — catches a wedged event loop where the
#    container itself still reports "running" but the app never responds.
#    Root cause of the 2026-09-10 incident: outbound network calls hung for
#    ~19h with zero log output, so check 1 (DNS resolved fine at the single
#    point-in-time it was tested) and the log-grep restart rules (which need
#    fresh matching log lines in a 15m window) never fired.
#    No host port is published for this container, and the image has no
#    curl — so hit it over the compose-internal bridge network by IP
#    instead of localhost:8000.
if [ "$status" = "running" ]; then
  backend_ip=$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$v.IPAddress}}{{end}}' "$CONTAINER" 2>/dev/null)
  if [ -n "$backend_ip" ]; then
    HEALTH_URL="http://$backend_ip:8000/health"
    if ! curl -fsS --max-time 10 "$HEALTH_URL" > /dev/null 2>&1; then
      sleep 5
      if ! curl -fsS --max-time 10 "$HEALTH_URL" > /dev/null 2>&1; then
        log "health check failed twice ($HEALTH_URL unreachable/timed out) — restarting"
        docker restart "$CONTAINER"
      fi
    fi
  fi
fi

# 4. Scrape-scheduler heartbeat — catches the warmup background task dying or
#    deadlocking while the HTTP server (check 3) keeps responding fine. This is
#    exactly how the 2026-10 missed-cycles incident went unnoticed: /health was
#    always OK, but the scheduler itself had stopped advancing for days.
#    Skipped during a grace period right after a (re)start, since a fresh
#    container needs a little time before its first heartbeat lands.
if [ "$status" = "running" ] && [ -n "$backend_ip" ]; then
  HEARTBEAT_GRACE=300
  started_at=$(docker inspect -f '{{.State.StartedAt}}' "$CONTAINER" 2>/dev/null)
  started_epoch=$(date -d "$started_at" +%s 2>/dev/null || echo 0)
  uptime=$(( $(date +%s) - started_epoch ))
  if [ "$uptime" -ge "$HEARTBEAT_GRACE" ]; then
    hb=$(curl -fsS --max-time 10 "http://$backend_ip:8000/warmup/heartbeat" 2>/dev/null)
    if [ -z "$hb" ]; then
      log "scrape heartbeat endpoint unreachable — restarting"
      docker restart "$CONTAINER"
    elif echo "$hb" | grep -q '"stale":true'; then
      age=$(echo "$hb" | grep -o '"age_seconds":[0-9.]*' | cut -d: -f2)
      log "scrape scheduler heartbeat stale (age=${age}s) — restarting"
      docker restart "$CONTAINER"
    fi
  fi
fi
