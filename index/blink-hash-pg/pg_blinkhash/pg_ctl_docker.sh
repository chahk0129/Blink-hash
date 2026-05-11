#!/usr/bin/env bash
# Simple wrapper to map pg_ctl start/stop to docker start/stop for blinkhash_pg
CONTAINER=${DOCKER_PG_CONTAINER:-blinkhash_pg}
cmd="$1"
shift || true
case "$cmd" in
  start)
    docker start "$CONTAINER" >/dev/null
    exit $?
    ;;
  stop)
    # if -m immediate is present, use docker kill
    if echo " $* " | grep -q -- -m\s*immediate; then
      docker kill "$CONTAINER" >/dev/null
      exit $?
    else
      docker stop -t 10 "$CONTAINER" >/dev/null
      exit $?
    fi
    ;;
  status)
    docker ps -q -f name="$CONTAINER" >/dev/null && exit 0 || exit 1
    ;;
  *)
    # fallback: run docker exec with provided args
    docker exec "$CONTAINER" pg_ctl "$cmd" "$@"
    ;;
esac
