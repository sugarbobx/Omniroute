#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

PYTHON_BIN="${PYTHON_BIN:-python3}"

case "${1:-}" in
  install|start|stop|restart|status)
    "$PYTHON_BIN" deploy.py "$@"
    ;;
  *)
    echo "Usage: ./deploy.sh {install|start|stop|restart|status} [--host HOST] [--port PORT] [--lifespan-on]"
    echo
    echo "Examples:"
    echo "  ./deploy.sh install"
    echo "  ./deploy.sh start --host 0.0.0.0 --port 8000"
    echo "  ./deploy.sh status"
    exit 2
    ;;
esac
