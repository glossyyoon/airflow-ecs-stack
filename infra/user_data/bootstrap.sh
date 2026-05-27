#!/bin/bash
# EC2 user_data — runs on first boot of the Airflow ECS host.
# Inputs come from terraform via templatefile():
#   - ${cluster_name}
#   - ${data_volume_id}
#   - ${aws_region}
#   - ${repo_url}
#
# Ordering matters: everything ECS depends on (mounts, secrets) must be in
# place BEFORE the git clone, because git clone is allowed to fail (the repo
# may not exist yet). If clone fails we still want a working Airflow with an
# empty DAG bag.
set -uo pipefail
exec > >(tee /var/log/airflow-bootstrap.log | logger -t airflow-bootstrap -s 2>/dev/console) 2>&1

CLUSTER_NAME="${cluster_name}"
DATA_VOLUME_ID="${data_volume_id}"
AWS_REGION="${aws_region}"
REPO_URL="${repo_url}"

# ----------------------------------------------------------------------
# 1. Register with ECS cluster
# ----------------------------------------------------------------------
echo "ECS_CLUSTER=$${CLUSTER_NAME}" >> /etc/ecs/ecs.config
echo "ECS_ENABLE_CONTAINER_METADATA=true" >> /etc/ecs/ecs.config

# ----------------------------------------------------------------------
# 2. Attach + format + mount EBS data volume
# ----------------------------------------------------------------------
# IMDSv2 is required (http_tokens=required in the launch template).
IMDS_TOKEN=$(curl -fs -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
INSTANCE_ID=$(curl -fs -H "X-aws-ec2-metadata-token: $${IMDS_TOKEN}" \
  http://169.254.169.254/latest/meta-data/instance-id)
echo "[bootstrap] instance-id=$${INSTANCE_ID}"

aws ec2 attach-volume \
  --region "$${AWS_REGION}" \
  --volume-id "$${DATA_VOLUME_ID}" \
  --instance-id "$${INSTANCE_ID}" \
  --device /dev/sdf || true

# Wait until the volume reports as attached, then find its block device.
for _ in $(seq 1 60); do
  STATE=$(aws ec2 describe-volumes --region "$${AWS_REGION}" \
    --volume-ids "$${DATA_VOLUME_ID}" \
    --query 'Volumes[0].Attachments[0].State' --output text 2>/dev/null || echo "")
  [ "$${STATE}" = "attached" ] && break
  sleep 2
done

DEV=""
for _ in $(seq 1 60); do
  DEV=$(lsblk -lnpo NAME,SIZE | awk '$2=="20G"{print $1; exit}')
  [ -n "$${DEV}" ] && break
  sleep 2
done
if [ -z "$${DEV}" ]; then
  echo "[bootstrap] WARNING: could not locate attached EBS volume; skipping postgres-data mount" >&2
fi

MOUNT=/srv/postgres-data
mkdir -p "$${MOUNT}"
if [ -n "$${DEV}" ]; then
  if ! blkid "$${DEV}" >/dev/null 2>&1; then
    mkfs.ext4 -F "$${DEV}"
  fi
  UUID=$(blkid -s UUID -o value "$${DEV}")
  grep -q "$${UUID}" /etc/fstab || echo "UUID=$${UUID} $${MOUNT} ext4 defaults,nofail 0 2" >> /etc/fstab
  mount -a
fi

# ----------------------------------------------------------------------
# 3. Airflow secrets file (.env) — MUST exist before ECS tries to mount it,
#    otherwise Docker creates a directory at the bind-mount path.
# ----------------------------------------------------------------------
ENV_FILE=/etc/airflow/.env
mkdir -p /etc/airflow

# If something (e.g. a previous failed boot via Docker bind-mount) created a
# directory at the .env path, blow it away before writing a real file.
if [ -d "$${ENV_FILE}" ]; then
  rmdir "$${ENV_FILE}" 2>/dev/null || rm -rf "$${ENV_FILE}"
fi

PG_PW_FILE=/etc/airflow/postgres-password
if [ -d "$${PG_PW_FILE}" ]; then
  rm -rf "$${PG_PW_FILE}"
fi

if [ ! -f "$${ENV_FILE}" ] || [ ! -f "$${PG_PW_FILE}" ]; then
  PW=$(openssl rand -hex 16)
  ADMIN_PW=$(openssl rand -hex 12)
  cat >"$${ENV_FILE}" <<EOF
POSTGRES_PASSWORD=$${PW}
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:$${PW}@postgres:5432/airflow
AIRFLOW_ADMIN_PASSWORD=$${ADMIN_PW}
EOF
  # Separate single-line file consumed by postgres via POSTGRES_PASSWORD_FILE.
  printf '%s' "$${PW}" >"$${PG_PW_FILE}"
fi
chown root:root "$${ENV_FILE}" "$${PG_PW_FILE}"
chmod 644 "$${ENV_FILE}" "$${PG_PW_FILE}"

# ----------------------------------------------------------------------
# 4. Airflow host directories — create them BEFORE git clone so that even
#    a failed clone leaves a valid (empty) dags/dbt mount point.
# ----------------------------------------------------------------------
mkdir -p /srv/airflow/{dags,dbt,logs,_repo}
chown -R 50000:0 /srv/airflow

# ----------------------------------------------------------------------
# 5. CloudWatch Agent (disk metrics) — independent of git, do it early.
# ----------------------------------------------------------------------
yum install -y amazon-cloudwatch-agent
cat >/opt/aws/amazon-cloudwatch-agent/etc/config.json <<'EOF'
{
  "agent": { "metrics_collection_interval": 60, "run_as_user": "cwagent" },
  "metrics": {
    "namespace": "Airflow/Host",
    "append_dimensions": {
      "InstanceId": "$${aws:InstanceId}"
    },
    "metrics_collected": {
      "disk": {
        "measurement": ["used_percent"],
        "resources": ["/", "/srv/postgres-data"],
        "metrics_collection_interval": 60
      },
      "mem": {
        "measurement": ["mem_used_percent"]
      }
    }
  }
}
EOF
amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s -c file:/opt/aws/amazon-cloudwatch-agent/etc/config.json || true

# ----------------------------------------------------------------------
# 6. git clone (non-fatal — repo may not exist yet). On failure we just
#    leave /srv/airflow/dags and /srv/airflow/dbt as empty dirs; Airflow
#    will start with an empty DAG bag and the UI will still come up.
# ----------------------------------------------------------------------
yum install -y git

if git clone "$${REPO_URL}" /srv/airflow/_repo; then
  # Replace dags/dbt with symlinks into the cloned repo, if those subdirs exist
  for sub in dags dbt; do
    if [ -d "/srv/airflow/_repo/$${sub}" ]; then
      rm -rf "/srv/airflow/$${sub}"
      ln -sfn "/srv/airflow/_repo/$${sub}" "/srv/airflow/$${sub}"
    fi
  done
  chown -h 50000:0 /srv/airflow/dags /srv/airflow/dbt

  # 7. systemd timer for periodic git pull (only worth setting up if clone worked)
  cat >/etc/systemd/system/git-sync.service <<EOF
[Unit]
Description=Pull latest DAG/dbt code from git
After=network-online.target

[Service]
Type=oneshot
ExecStart=/usr/bin/git -C /srv/airflow/_repo pull --ff-only
EOF

  cat >/etc/systemd/system/git-sync.timer <<EOF
[Unit]
Description=Periodic Airflow repo git pull

[Timer]
OnBootSec=1min
OnUnitActiveSec=5min
Unit=git-sync.service

[Install]
WantedBy=timers.target
EOF

  systemctl daemon-reload
  systemctl enable --now git-sync.timer
else
  echo "[bootstrap] git clone of $${REPO_URL} failed — leaving empty dags/dbt dirs; ECS task will still start." >&2
fi

# ----------------------------------------------------------------------
# 8. Restart ECS so it picks up cluster config changes
# ----------------------------------------------------------------------
systemctl restart ecs
