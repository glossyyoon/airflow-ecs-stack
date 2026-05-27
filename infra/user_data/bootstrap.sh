#!/bin/bash
# EC2 user_data — runs on first boot of the Airflow ECS host.
# Inputs come from terraform via templatefile():
#   - ${cluster_name}
#   - ${data_volume_id}
#   - ${aws_region}
#   - ${repo_url}
set -euo pipefail
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
INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
aws ec2 attach-volume \
  --region "$${AWS_REGION}" \
  --volume-id "$${DATA_VOLUME_ID}" \
  --instance-id "$${INSTANCE_ID}" \
  --device /dev/sdf || true

# Wait for the kernel to expose the volume (NVMe rename)
for _ in $(seq 1 60); do
  DEV=$(lsblk -lnpo NAME,SIZE | awk '$2=="20G"{print $1; exit}')
  [ -n "$${DEV:-}" ] && break
  sleep 2
done
if [ -z "$${DEV:-}" ]; then
  echo "failed to locate attached EBS volume" >&2
  exit 1
fi

MOUNT=/srv/postgres-data
mkdir -p "$${MOUNT}"
if ! blkid "$${DEV}" >/dev/null 2>&1; then
  mkfs.ext4 -F "$${DEV}"
fi
UUID=$(blkid -s UUID -o value "$${DEV}")
grep -q "$${UUID}" /etc/fstab || echo "UUID=$${UUID} $${MOUNT} ext4 defaults,nofail 0 2" >> /etc/fstab
mount -a

# ----------------------------------------------------------------------
# 3. Airflow host directories + clone repo
# ----------------------------------------------------------------------
yum install -y git
mkdir -p /srv/airflow/{logs,_repo}
chown -R 50000:0 /srv/airflow

if [ ! -d /srv/airflow/_repo/.git ]; then
  git clone "$${REPO_URL}" /srv/airflow/_repo
fi
ln -sfn /srv/airflow/_repo/dags /srv/airflow/dags
ln -sfn /srv/airflow/_repo/dbt  /srv/airflow/dbt
chown -h 50000:0 /srv/airflow/dags /srv/airflow/dbt

# ----------------------------------------------------------------------
# 4. systemd timer for periodic git pull
# ----------------------------------------------------------------------
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

# ----------------------------------------------------------------------
# 5. /etc/airflow/.env (preserve if present)
# ----------------------------------------------------------------------
ENV_FILE=/etc/airflow/.env
mkdir -p /etc/airflow
if [ ! -f "$${ENV_FILE}" ]; then
  PW=$(openssl rand -hex 16)
  ADMIN_PW=$(openssl rand -hex 12)
  cat >"$${ENV_FILE}" <<EOF
POSTGRES_PASSWORD=$${PW}
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:$${PW}@postgres:5432/airflow
AIRFLOW_ADMIN_PASSWORD=$${ADMIN_PW}
EOF
  chmod 600 "$${ENV_FILE}"
fi
# The container-side UID 50000 needs read access; group-readable to the airflow group only.
chown root:root "$${ENV_FILE}"
chmod 644 "$${ENV_FILE}"

# ----------------------------------------------------------------------
# 6. CloudWatch Agent (disk metrics)
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
amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s -c file:/opt/aws/amazon-cloudwatch-agent/etc/config.json

# Restart ECS so it picks up cluster config changes
systemctl restart ecs
