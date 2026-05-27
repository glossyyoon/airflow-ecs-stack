# airflow-ecs-stack

Airflow 3.2.1 on ECS-EC2 in **ap-northeast-2**, with a Glue Python Shell
"smoke test" job that proves Airflow's `GlueJobOperator` can start a Glue
job, wait for it, and propagate success/failure. A dbt (Cosmos) skeleton
is wired into the DAG but skipped pending a real Redshift target.

> Original scope of "Polars on Python 3.11 inside Glue" was dropped because
> AWS Glue Python Shell currently caps at Python 3.9 (the API regex
> rejects `3.11` for `command.pythonVersion`). The smoke test here uses a
> stdlib + boto3 stub that just counts input objects and writes a marker
> — enough to validate the Airflow ↔ Glue invocation contract. Picking up
> Polars-on-3.11 would require switching the runtime (Glue ETL Spark 5.0,
> Lambda, or Fargate) and is left as a follow-up.

Design rationale lives in
[`docs/superpowers/specs/2026-05-27-airflow-ecs-glue-design.md`](docs/superpowers/specs/2026-05-27-airflow-ecs-glue-design.md).

---

## Layout

```
docker/             Custom Airflow image (Dockerfile + entrypoint)
dags/               Airflow DAGs (bind-mounted into the container via git pull)
dbt/                dbt project skeleton + profiles.yml template
glue/               Glue Python Shell stub (etl_stub.py)
scripts/            Local helpers: mock data generator, output inspector
infra/terraform/    All AWS resources (VPC, IAM, EC2, ECS, S3, Glue, CloudWatch)
infra/user_data/    EC2 bootstrap (EBS attach, git clone, .env, CWAgent)
.github/workflows/  Docker build/push and Terraform plan/apply
```

---

## Bootstrap

Prerequisites: AWS account with admin-ish rights, Terraform >= 1.6, Docker, a
public git repo URL the EC2 host can `git clone` from.

```bash
# 1. Plan & apply
cd infra/terraform
terraform init
terraform apply \
  -var "repo_url=https://github.com/<org>/airflow-ecs-stack.git" \
  -var 'ssh_admin_cidrs=["<your.dev.ip>/32"]' \
  -var 'ui_allowed_cidrs=["<your.dev.ip>/32","<your.browser.ip>/32"]'

# 2. Push the custom Airflow image
ECR=$(terraform output -raw ecr_repository_url)
aws ecr get-login-password --region ap-northeast-2 \
  | docker login --username AWS --password-stdin "${ECR%/*}"
docker build -t "$ECR:3.2.1-py3.11" ../../docker
docker push "$ECR:3.2.1-py3.11"

# 3. Force the ECS service to pull the new image
aws ecs update-service \
  --cluster airflow-cluster --service airflow-svc --force-new-deployment
```

The Glue stub script is uploaded by `terraform apply` itself (see `s3.tf`),
so the DAG is end-to-end runnable once the service is healthy.

Airflow UI: `http://<ec2-public-ip>:8080`. Admin password is generated at
container startup by Airflow 3's `SimpleAuthManager` and printed to the
container logs (`/ecs/airflow` log group, stream `airflow/airflow/<task-id>`,
grep for `"Password for user 'admin'"`).

---

## Running the smoke test

```bash
# upload a tiny mock raw partition (50k parquet rows — content not used by stub)
python -m venv .venv && source .venv/bin/activate
pip install -r scripts/requirements.txt
python scripts/generate_mock_data.py \
  --bucket airflow-ecs-raw-<acct> --date $(date -u +%F)

# in UI: trigger DAG `glue_smoke_test` (or via API)
# after success, inspect the curated bucket
python scripts/inspect_curated.py \
  --bucket airflow-ecs-curated-<acct> --date $(date -u +%F)
```

Expected output of `inspect_curated.py`:
```
objects under s3://airflow-ecs-curated-<acct>/dt=YYYY-MM-DD/:
  dt=YYYY-MM-DD/summary.txt   ... bytes
  dt=YYYY-MM-DD/_SUCCESS              0B
summary.txt:
  run_id=manual__...
  source_object_count=2
  glue_runtime=python-shell-3.9
```

---

## Daily workflow

| Change | What happens |
|---|---|
| New / edited DAG | `git push` → host `git-sync.timer` pulls in ≤ 5 min → Airflow auto-loads |
| New dbt model | same as above — Cosmos picks it up at the next DAG run |
| New Python dep / Airflow upgrade | Rebuild the image, push, then `aws ecs update-service --force-new-deployment` |
| New Glue logic | Edit `glue/etl_stub.py`, `terraform apply` (re-uploads via S3 etag) |

---

## What this stack does **not** include

- Backup/recovery automation
- Redshift cluster (only the dbt/Cosmos hook is configured)
- Polars / Python 3.11 in Glue (see top-of-file note)
- Fargate, NAT Gateway, VPC Endpoints
- ALB/HTTPS termination, SAML SSO
- Multi-AZ HA, multiple workers
- Corporate proxy egress
