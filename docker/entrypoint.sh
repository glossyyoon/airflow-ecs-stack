#!/usr/bin/env bash
set -euo pipefail

SECRETS_FILE="/run/secrets/airflow.env"
if [[ -f "${SECRETS_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "${SECRETS_FILE}"
  set +a
fi

: "${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:?must be set via /run/secrets/airflow.env}"

wait_for_db() {
  for _ in $(seq 1 60); do
    if airflow db check >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "[entrypoint] timed out waiting for metadata DB" >&2
  return 1
}

wait_for_db
airflow db migrate

if ! airflow users list 2>/dev/null | grep -q '^admin '; then
  airflow users create \
    --role Admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@example.local \
    --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" || true
fi

run_prefixed() {
  local label="$1"
  shift
  "$@" 2>&1 | sed -u "s/^/[${label}] /"
}

run_prefixed scheduler     airflow scheduler     &
PID_SCHEDULER=$!
run_prefixed triggerer     airflow triggerer     &
PID_TRIGGERER=$!
run_prefixed api           airflow api-server    &
PID_API=$!
# Airflow 3 split DAG parsing out of the scheduler into its own daemon.
# Without this the UI shows "DAG processor not connected".
run_prefixed dag-processor airflow dag-processor &
PID_DAG_PROCESSOR=$!

shutdown() {
  kill -TERM "${PID_SCHEDULER}" "${PID_TRIGGERER}" "${PID_API}" "${PID_DAG_PROCESSOR}" 2>/dev/null || true
  wait
}
trap shutdown SIGTERM SIGINT

wait -n
exit_code=$?
shutdown
exit "${exit_code}"
