# airflow-ecs-stack

Airflow 3.2.1 on ECS-EC2 in **ap-northeast-2**, triggering a Glue Python Shell
job that runs Polars on Python 3.11, with a dbt (Cosmos) skeleton ready to be
pointed at Redshift later.

Design rationale lives in
[`docs/superpowers/specs/2026-05-27-airflow-ecs-glue-design.md`](docs/superpowers/specs/2026-05-27-airflow-ecs-glue-design.md).

---

## Layout

```
docker/             Custom Airflow image (Dockerfile + entrypoint)
dags/               Airflow DAGs (bind-mounted into the container via git pull)
dbt/                dbt project skeleton + profiles.yml template
glue/               Glue Python Shell entrypoint (polars_etl.py)
infra/terraform/    All AWS resources (VPC, IAM, EC2, ECS, S3, Glue, CloudWatch)
infra/user_data/    EC2 bootstrap (EBS attach, git clone, .env, CWAgent)
.github/workflows/  Docker build/push and Terraform plan/apply
```

---

## Bootstrap

Prerequisites: AWS account with admin-ish rights, Terraform >= 1.6, Docker, a
git repo URL the EC2 host can `git clone` from (HTTPS, no auth or PAT-in-URL).

```bash
# 1. Plan & apply (creates VPC, EC2 with desiredCount=1 service, ECR, S3, Glue, …)
cd infra/terraform
terraform init
terraform apply \
  -var "repo_url=https://github.com/<org>/airflow-ecs-stack.git" \
  -var "ssh_admin_cidr=$(curl -s ifconfig.me)/32" \
  -var "ui_allowed_cidr=$(curl -s ifconfig.me)/32"

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

The Glue job script is uploaded by `terraform apply` itself (see `s3.tf`), so
the DAG is end-to-end runnable once the service is healthy.

Airflow UI: `http://<ec2-public-ip>:8080` (admin password is generated at first
boot and stored in `/etc/airflow/.env` on the host — fetch it via SSM Session
Manager).

---

## Daily workflow

| Change | What happens |
|---|---|
| New / edited DAG | `git push` → host `git-sync.timer` pulls in ≤ 5 min → Airflow auto-loads |
| New dbt model | same as above — Cosmos picks it up at the next DAG run |
| New Python dep / Airflow upgrade | Rebuild the image, push, then `aws ecs update-service --force-new-deployment` |
| New Glue logic | Edit `glue/polars_etl.py`, `terraform apply` (re-uploads via S3 etag) |

---

## What this stack does **not** include

Intentional scope cuts (see design §7):

- Backup/recovery automation
- Redshift cluster (only the dbt/Cosmos hook is configured)
- Fargate, NAT Gateway, VPC Endpoints
- ALB/HTTPS termination, SAML SSO
- Multi-AZ HA, multiple workers
- Corporate proxy egress

Each of these can be layered on as a follow-up sub-project.
