# Airflow on ECS-EC2 + DBT(Cosmos) + Glue(Polars) — 아키텍처 설계

- 날짜: 2026-05-27
- 리전: ap-northeast-2 (Seoul)
- 범위: End-to-end 아키텍처 설계만 (IaC/코드 구현은 후속 단계)
- 산출 레포: `airflow-ecs-stack` (이 문서가 들어있는 레포)

---

## 0. 결정 요약

| 항목 | 선택 |
|---|---|
| ECS 실행 타입 | ECS on EC2 (Fargate 미사용) |
| Airflow | 3.2.1 / python 3.11 / LocalExecutor |
| 메타데이터 DB | 동일 EC2 위의 Postgres 컨테이너 + EBS |
| DAG / dbt 코드 배포 | EC2 호스트 디렉터리 bind mount + systemd timer git pull |
| Glue | Glue Python Shell 5.0, Python 3.11 + Polars. **잡 정의도 이 설계가 만든다** |
| DBT | Cosmos + dbt-redshift 설치 + 스켈레톤 프로젝트. **Redshift 연결 자체는 본 설계 범위 외** |
| AWS 인증 | EC2 instance profile + boto3 기본 체인 (Airflow Connection 미사용) |
| 네트워크 | 신규 VPC / 단일 public subnet / IGW / EC2 퍼블릭 IP. 프록시 미사용 |
| 운영 가시성 | CloudWatch Logs + 최소 알람 4종 + SNS topic |

본 설계가 만들지 **않는 것**: Redshift, Fargate, NAT Gateway, VPC endpoints, ALB/HTTPS 종단, SAML SSO, 멀티-AZ HA, 백업/복구 자동화.

---

## 1. 컴포넌트 명세

### 1.1 EC2 호스트
- AMI: ECS-optimized Amazon Linux 2023 (Seoul 리전 최신).
- 인스턴스 타입: `t3.large` (vCPU 2 / RAM 8GB).
- EBS:
  - root: gp3 30GB (OS, 도커 이미지)
  - data: gp3 20GB, 별도 마운트 `/srv/postgres-data` — Postgres 컨테이너 전용
- 호스트 디렉터리:
  - `/srv/airflow/dags` — DAG 파일 (git pull 대상)
  - `/srv/airflow/dbt`  — dbt 프로젝트 (git pull 대상)
  - `/srv/airflow/logs` — Airflow task 로그 (CloudWatch 보조)

### 1.2 ECS 클러스터
- 클러스터: `airflow-cluster`
- Capacity provider: Auto Scaling Group 1대 (min=max=1)
- 서비스: `airflow-svc` (desiredCount=1)
- Task definition: `airflow-task`, network mode **bridge** (동일 task 내 컨테이너끼리 `links` / `localhost` 통신)

### 1.3 단일 Task / 두 컨테이너

| 컨테이너 | 이미지 | 포트 | 마운트 | 핵심 환경변수 |
|---|---|---|---|---|
| `airflow` | ECR `airflow-custom:3.2.1-py3.11` | 8080 → host 8080 | `/srv/airflow/dags` → `/opt/airflow/dags` (ro)<br>`/srv/airflow/dbt` → `/opt/airflow/dbt` (ro)<br>`/srv/airflow/logs` → `/opt/airflow/logs`<br>`/etc/airflow/.env` → `/run/secrets/airflow.env` (ro) | `AIRFLOW__CORE__EXECUTOR=LocalExecutor`<br>`AIRFLOW__CORE__LOAD_EXAMPLES=False`<br>`AWS_DEFAULT_REGION=ap-northeast-2`<br>(DB conn / 비밀번호는 §3.7 .env에서 로드) |
| `postgres` | `postgres:16` | 5432 (task 내부) | `/srv/postgres-data` → `/var/lib/postgresql/data`<br>`/etc/airflow/.env` → `/run/secrets/airflow.env` (ro) | `POSTGRES_USER=airflow`<br>`POSTGRES_DB=airflow`<br>(`POSTGRES_PASSWORD`은 §3.7 .env에서 로드) |

Airflow 컨테이너 entrypoint는 `airflow standalone` 대신 production-safe 형태로
`airflow db migrate` → `airflow users create`(idempotent) → `airflow api-server` / `airflow scheduler` / `airflow triggerer`를 동시 기동
(`tini` + 단일 shell script로 세 프로세스 supervise).

### 1.4 Airflow 커스텀 이미지

```dockerfile
FROM docker.io/apache/airflow:3.2.1-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential libpq-dev unzip libaio1 wget libffi-dev libssl-dev git \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
ENV PATH="${PATH}:/home/airflow/.local/bin"

RUN pip install --no-cache-dir --upgrade pip setuptools wheel packaging

RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      oracledb==3.3.0 \
      "astronomer-cosmos[dbt-redshift]==1.14.1" \
      dbt-core \
      dbt-redshift \
      apache-airflow-providers-amazon \
      awscli

RUN dbt --version && pip show oracledb | grep Version

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
```

원본 대비 변경점:

- `HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY` 환경변수 및 `pip install --proxy ""` 옵션 제거 (프록시 미사용).
- `apache-airflow-providers-amazon` 추가 — Airflow 3.x에서 `GlueJobOperator` / S3 sensor는 별도 provider 패키지에 존재. 원본 파일에는 누락되어 있어 Glue 호출 불가.

---

## 2. 데이터 흐름 / DAG 패턴

### 2.1 런타임 흐름

```
[Airflow scheduler]
      │
      │ ① schedule 도달 / 수동 trigger
      ▼
[DAG: s3_polars_etl]
      │
      ├─ ② S3KeySensor   raw/dt=YYYY-MM-DD/_SUCCESS 대기
      │        poke_interval=60s, timeout=30m, mode=reschedule
      │
      ├─ ③ GlueJobOperator
      │        job_name = "polars-etl-prd"
      │        script_args = {"--input": "s3://acme-raw/dt=…",
      │                       "--output":"s3://acme-curated/dt=…",
      │                       "--run_id": "{{ run_id }}"}
      │        wait_for_completion = True
      │        boto3 ← EC2 instance role
      │
      ├─ ④ S3KeySensor   curated/dt=YYYY-MM-DD/_SUCCESS 검증
      │
      └─ ⑤ DbtTaskGroup (Cosmos) — "config only" 스켈레톤
               select=["tag:enabled"] (현재 매칭 0건, 항상 success)
```

### 2.2 핵심 결정

**Glue 호출 = `GlueJobOperator(wait_for_completion=True)`**
- `aws_conn_id=None` → boto3 기본 체인 → EC2 instance role.
- 동기 대기로 task slot을 점유하지만 동시성이 낮아 허용.
- 동시 실행이 늘면 `deferrable=True`로 한 줄 변경하여 triggerer로 이양.

**dbt = Cosmos `DbtTaskGroup` "스켈레톤"**
- 본 설계 범위는 "Cosmos 설치 + minimal dbt 프로젝트 + Airflow task group으로 노출"까지.
- `dbt/sample_project/`에 `dbt_project.yml`, 빈 `models/`, `profiles.yml` 템플릿(host/user/password는 env로).
- `render_config={"select": ["tag:enabled"]}` 로 두면 현재 매칭 모델 0건 → 항상 success. Redshift 연결 단계에서 tag만 풀어주면 활성화.

**스케줄 / 멱등성**
- `schedule="0 2 * * *"`, `catchup=False`, `max_active_runs=1`
- Glue 인자에 날짜 파티션과 `--run_id={{ run_id }}` → 같은 날짜 재실행 시 출력 위치 동일 → 멱등.
- DAG `retries=2`, `retry_delay=5m`, `retry_exponential_backoff=True`.

**실패 처리**
- Sensor 타임아웃 / Glue FAILED → operator가 raise → retry 규칙 적용.
- `on_failure_callback`로 CloudWatch 로그 그룹에 메시지 기록 (SNS 알림은 §4.5).

### 2.3 예시 DAG (의사 코드)

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig

with DAG(
    dag_id="s3_polars_etl",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:

    wait_raw = S3KeySensor(
        task_id="wait_raw",
        bucket_key="s3://acme-raw/dt={{ ds }}/_SUCCESS",
        mode="reschedule", poke_interval=60, timeout=60*30,
    )

    run_glue = GlueJobOperator(
        task_id="run_glue_polars",
        job_name="polars-etl-prd",
        script_args={
            "--input":  "s3://acme-raw/dt={{ ds }}/",
            "--output": "s3://acme-curated/dt={{ ds }}/",
            "--run_id": "{{ run_id }}",
        },
        wait_for_completion=True,
        aws_conn_id=None,           # boto3 default chain → EC2 instance role
        region_name="ap-northeast-2",
    )

    verify_curated = S3KeySensor(
        task_id="verify_curated",
        bucket_key="s3://acme-curated/dt={{ ds }}/_SUCCESS",
        mode="reschedule", poke_interval=30, timeout=60*10,
    )

    dbt = DbtTaskGroup(
        group_id="dbt_models",
        project_config=ProjectConfig("/opt/airflow/dbt/sample_project"),
        profile_config=ProfileConfig(
            profile_name="acme",
            target_name="dev",
            profiles_yml_filepath="/opt/airflow/dbt/profiles.yml",
        ),
        render_config={"select": ["tag:enabled"]},
    )

    wait_raw >> run_glue >> verify_curated >> dbt
```

---

## 3. 보안 / IAM

### 3.1 만드는 역할 4종

| # | 역할 | 부착 위치 | 누가 assume |
|---|---|---|---|
| 1 | `ec2-airflow-host-role` | EC2 instance profile | EC2 호스트 (IMDS) → 컨테이너 상속 |
| 2 | `ecs-task-execution-role` | task definition `executionRoleArn` | ECS 에이전트 |
| 3 | `glue-polars-job-role` | Glue job 정의 `Role` | `glue.amazonaws.com` |
| 4 | (정책만) `iam:PassRole` for #3 | #1 role에 inline 추가 | — Airflow가 `StartJobRun` 시 검증 통과용 |

### 3.2 `ec2-airflow-host-role`

Trust:
```json
{ "Version":"2012-10-17",
  "Statement":[{ "Effect":"Allow",
                 "Principal":{"Service":"ec2.amazonaws.com"},
                 "Action":"sts:AssumeRole" }] }
```

Inline `airflow-runtime`:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "GlueInvoke",
      "Effect": "Allow",
      "Action": [
        "glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns",
        "glue:GetJob", "glue:BatchStopJobRun"
      ],
      "Resource": "arn:aws:glue:ap-northeast-2:<ACCOUNT_ID>:job/polars-etl-prd" },

    { "Sid": "PassGlueRoleToGlue",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::<ACCOUNT_ID>:role/glue-polars-job-role",
      "Condition": { "StringEquals": { "iam:PassedToService": "glue.amazonaws.com" } } },

    { "Sid": "S3SensorList",
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": ["arn:aws:s3:::acme-raw", "arn:aws:s3:::acme-curated"] },

    { "Sid": "S3SensorObjects",
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::acme-raw/*", "arn:aws:s3:::acme-curated/*"] },

    { "Sid": "AirflowLogs",
      "Effect": "Allow",
      "Action": ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"],
      "Resource": "arn:aws:logs:ap-northeast-2:<ACCOUNT_ID>:log-group:/airflow/*" }
  ]
}
```

Managed 추가 attach:
- `AmazonSSMManagedInstanceCore` — SSM Session Manager 접근 (SSH 대안).
- `AmazonEC2ContainerServiceforEC2Role` — ECS 에이전트 등록/통신.

### 3.3 `ecs-task-execution-role`

- Trust: `ecs-tasks.amazonaws.com` → `sts:AssumeRole`
- Managed: `AmazonECSTaskExecutionRolePolicy` (ECR pull + CloudWatch Logs put)

### 3.4 `glue-polars-job-role`

Trust:
```json
{ "Version":"2012-10-17",
  "Statement":[{ "Effect":"Allow",
                 "Principal":{"Service":"glue.amazonaws.com"},
                 "Action":"sts:AssumeRole" }] }
```

Managed: `AWSGlueServiceRole`

Inline `glue-polars-data-access`:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "ReadRaw",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::acme-raw", "arn:aws:s3:::acme-raw/*"] },

    { "Sid": "WriteCurated",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
        "s3:AbortMultipartUpload", "s3:ListBucket", "s3:ListBucketMultipartUploads"
      ],
      "Resource": ["arn:aws:s3:::acme-curated", "arn:aws:s3:::acme-curated/*"] },

    { "Sid": "GlueAssetsBucket",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::acme-glue-assets", "arn:aws:s3:::acme-glue-assets/*"] }
  ]
}
```

> 버킷 이름은 자리표시자(`acme-*`). 실제 이름은 IaC 변수로 분리.
> S3 KMS 적용 시 `kms:Decrypt`, `kms:GenerateDataKey`를 해당 키 ARN으로 추가.

### 3.5 Glue Job 리소스 (설계가 만든다)

```hcl
glue_job "polars-etl-prd" {
  role_arn          = aws_iam_role.glue_polars_job_role.arn
  command {
    name            = "pythonshell"
    python_version  = "3.11"
    script_location = "s3://acme-glue-assets/jobs/polars_etl.py"
  }
  default_arguments = {
    "--additional-python-modules"        = "polars==1.18.0,pyarrow==18.1.0"
    "--TempDir"                          = "s3://acme-glue-assets/tmp/"
    "--enable-continuous-cloudwatch-log" = "true"
  }
  max_capacity = 1.0    # Glue Python Shell DPU
  glue_version = "5.0"
  timeout      = 60     # 분
  max_retries  = 0      # Airflow가 재시도 책임
}
```

스크립트 객체 `s3://acme-glue-assets/jobs/polars_etl.py`는 IaC의
`aws_s3_object` 리소스로 stub 파일을 업로드하거나, 별도 CI에서 푸시. 두 옵션 모두 허용.

### 3.6 보안 그룹

```
sg-airflow-host  (EC2에 부착)
  Inbound:
    - tcp/22   from <관리자_IP>/32          (SSH; SSM Session Manager 권장)
    - tcp/8080 from <사내_IP_CIDR>          (Airflow UI)
  Outbound:
    - all → 0.0.0.0/0                      (AWS API, ECR, S3, github)
```

- ALB / HTTPS 종단은 본 설계 범위 외 — 운영 단계에서 옵션.
- Postgres 포트는 호스트에 노출하지 않음. SG 규칙 불필요.

### 3.7 시크릿

- Airflow 메타 DB 비밀번호와 admin 초기 비밀번호: EC2 `user_data`가 부팅 시 `/etc/airflow/.env`에 랜덤 생성 (이미 있으면 보존).
- ECS task definition은 `/etc/airflow/.env`를 두 컨테이너에 **bind mount (read-only)** — `/run/secrets/airflow.env` 경로로.
- 두 컨테이너의 entrypoint script가 시작 시 `set -a; . /run/secrets/airflow.env; set +a` 로 env 로드 후 정식 프로세스 기동.
- 단일 노드 단순화 결정. 멀티 노드 / 감사 요건이 생기면 AWS Secrets Manager + task definition `secrets` 필드 (또는 S3 호스팅 `environmentFiles`)로 전환.

### 3.8 Airflow 인증

- 기본 인증 = FAB Auth Manager.
- 초기 admin 계정: entrypoint script가 1회 `airflow users create`, 비밀번호는 `/etc/airflow/.env`.
- SSO/SAML 미포함.

---

## 4. 로깅 / 모니터링

### 4.1 로그 갈래 3개

| 갈래 | 출처 | 전송 경로 | CWLogs group |
|---|---|---|---|
| A | ECS 컨테이너 stdout/stderr | awslogs 드라이버 | `/ecs/airflow` (streams: `airflow/…`, `postgres/…`) |
| B | Airflow task 실행 로그 | Airflow remote logging | `/airflow/tasks` |
| C | Glue 잡 로그 | Glue Continuous Logging | `/aws-glue/python-jobs/output`, `…/error` |

### 4.2 A — ECS 컨테이너 로그

```json
"logConfiguration": {
  "logDriver": "awslogs",
  "options": {
    "awslogs-group": "/ecs/airflow",
    "awslogs-region": "ap-northeast-2",
    "awslogs-stream-prefix": "airflow",
    "awslogs-create-group": "true"
  }
}
```

- Retention 30일.
- Airflow 컨테이너에서 scheduler/api-server/triggerer가 stdout 공유 → entrypoint가 각 프로세스에 `[scheduler]` / `[api]` / `[triggerer]` 라인 prefix 부여.

### 4.3 B — Airflow task 로그 → CloudWatch

```ini
AIRFLOW__LOGGING__REMOTE_LOGGING = True
AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER = cloudwatch://arn:aws:logs:ap-northeast-2:<ACCOUNT_ID>:log-group:/airflow/tasks
AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID =
AIRFLOW__LOGGING__ENCRYPT_S3_LOGS = False
AIRFLOW__LOGGING__LOGGING_LEVEL = INFO
```

- Conn id 비움 → boto3 기본 체인 → instance role.
- 호스트 bind mount `/srv/airflow/logs`는 CWLogs 송신 실패 시 fallback.
- Retention 30일.
- 필요 권한은 §3.2의 `AirflowLogs` statement.

### 4.4 C — Glue 잡 로그

§3.5의 `--enable-continuous-cloudwatch-log=true`로 자동 활성. Airflow는 task log에 Glue job run ID와 CloudWatch URL을 자동 print (`GlueJobOperator` 기본 동작).

### 4.5 최소 알람 (4종)

| Alarm | Metric | 임계치 | 조치 |
|---|---|---|---|
| `airflow-instance-status` | EC2 `StatusCheckFailed` | > 0 for 2dp (5m) | EC2 자동 복구 |
| `airflow-cpu-high` | EC2 `CPUUtilization` | > 85% for 15m | 알림 |
| `airflow-disk-low` | CWAgent `disk_used_percent{path=/srv/postgres-data}` | > 80% | 알림 |
| `glue-polars-etl-failed` | EventBridge rule on Glue Job State Change = FAILED | > 0 | 알림 |

- CWAgent는 user_data가 설치 + conf 배포.
- 알림 대상: SNS topic `airflow-ops` (구독자는 비워 둠, 운영자가 콘솔에서 등록).

### 4.6 헬스체크

```json
"healthCheck": {
  "command": ["CMD-SHELL", "curl -fsS http://localhost:8080/health || exit 1"],
  "interval": 30, "timeout": 5, "retries": 3, "startPeriod": 90
}
```

- Airflow 3.x `/health`는 scheduler/triggerer heartbeat 포함.
- Postgres 컨테이너는 `pg_isready` 기반 healthcheck.
- 두 컨테이너 모두 `essential: true` → 하나라도 죽으면 task 재시작 → 서비스가 새 task 생성.

---

## 5. 운영

### 5.1 부트스트랩 순서 (한 번)

```
① terraform apply
   - VPC / public subnet / IGW / route table
   - SG (sg-airflow-host)
   - IAM roles 4종 (§3.1)
   - ECR repo: airflow-custom
   - S3 buckets: acme-raw / acme-curated / acme-glue-assets
   - EBS volume (20GB gp3, 미부착 생성)
   - EC2 (user_data 포함, EBS attach까지)
   - ECS cluster + capacity provider (ASG 1)
   - ECS task definition + service (desiredCount=0)
   - CloudWatch log groups + retention
   - CloudWatch alarms + SNS topic
   - Glue job 정의 (script_location은 stub 가리킴)

② 이미지 빌드 & 푸시
   docker build -t airflow-custom:3.2.1-py3.11 .
   docker push <ecr>/airflow-custom:3.2.1-py3.11

③ Glue 스크립트 업로드
   aws s3 cp polars_etl.py s3://acme-glue-assets/jobs/

④ ECS service desiredCount=1
   → task 부팅 → entrypoint가 airflow db migrate + admin user 생성 (idempotent)
```

### 5.2 EC2 `user_data` 핵심 작업

```bash
#!/bin/bash
set -e

# 1) ECS 클러스터 등록
echo "ECS_CLUSTER=airflow-cluster" >> /etc/ecs/ecs.config

# 2) EBS 데이터 볼륨 마운트
DEV=/dev/nvme1n1
MOUNT=/srv/postgres-data
[ "$(file -s $DEV)" = "$DEV: data" ] && mkfs.ext4 $DEV
mkdir -p $MOUNT
echo "$DEV $MOUNT ext4 defaults,nofail 0 2" >> /etc/fstab
mount -a

# 3) Airflow 호스트 디렉터리 + git clone
mkdir -p /srv/airflow/{dags,dbt,logs}
chown -R 50000:0 /srv/airflow
git clone https://github.com/<org>/airflow-ecs-stack.git /srv/airflow/_repo
ln -sfn /srv/airflow/_repo/dags /srv/airflow/dags
ln -sfn /srv/airflow/_repo/dbt  /srv/airflow/dbt

# 4) 5분 주기 git pull (systemd timer)
cat >/etc/systemd/system/git-sync.timer <<EOF
[Unit] Description=git pull airflow repo
[Timer] OnBootSec=1min OnUnitActiveSec=5min Unit=git-sync.service
[Install] WantedBy=timers.target
EOF
cat >/etc/systemd/system/git-sync.service <<EOF
[Service] Type=oneshot
ExecStart=/usr/bin/git -C /srv/airflow/_repo pull --ff-only
EOF
systemctl enable --now git-sync.timer

# 5) .env 생성 (보존 처리)
ENV_FILE=/etc/airflow/.env
mkdir -p /etc/airflow
if [ ! -f $ENV_FILE ]; then
  PW=$(openssl rand -hex 16)
  cat >$ENV_FILE <<EOF
POSTGRES_PASSWORD=$PW
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:$PW@postgres:5432/airflow
AIRFLOW_ADMIN_PASSWORD=$(openssl rand -hex 12)
EOF
  chmod 600 $ENV_FILE
fi

# 6) CloudWatch Agent (디스크 메트릭)
yum install -y amazon-cloudwatch-agent
amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s \
  -c file:/opt/aws/amazon-cloudwatch-agent/etc/config.json
```

### 5.3 일상 배포 (DAG / dbt 변경)

```
[개발자]   git push  →  GitHub
[EC2]     systemd timer가 5분 내 git pull
[Airflow] DAG processor가 변경 자동 반영, 재시작 불필요
[dbt]     Cosmos가 다음 DAG run에서 변경된 manifest 사용
```

이미지 변경 없음. 라이브러리 추가 시에만 §5.4.

### 5.4 이미지 / Airflow 업그레이드

```
1. Dockerfile 수정 → 새 태그 빌드 → ECR 푸시
2. terraform: task definition의 image 태그 변경 → apply
3. ECS rolling update (단일 task라 1~2분 중단 후 재기동)
4. 새 task의 entrypoint가 airflow db migrate 자동 수행
```

Postgres 메이저 업그레이드(16→17 등)는 데이터 디렉터리 호환성 때문에
같은 EBS에 그대로 못 올림 → 별도 절차 필요. 본 설계는 16에서 시작한다고만 명시.

### 5.5 비용 가늠 (월, ap-northeast-2 개략)

| 항목 | 단가/규모 | 월 |
|---|---|---|
| EC2 t3.large 24/7 | $0.1216 × 730h | ~$89 |
| EBS gp3 50GB | $0.0912 × 50 | ~$5 |
| CloudWatch Logs (3 group) | 1~3GB/월 가정 | ~$5 |
| Glue Python Shell 1DPU × 1h/일 | $0.44 × 30 | ~$13 |
| S3 storage / 요청 | 가변 | — |
| **합계 (S3 가변 제외)** | | **~$110/월** |

가격은 변동.

---

## 6. 레포 레이아웃

```
airflow-ecs-stack/
├── README.md
├── docs/
│   └── superpowers/
│       └── specs/
│           └── 2026-05-27-airflow-ecs-glue-design.md   ← 본 문서
├── infra/
│   ├── terraform/
│   │   ├── main.tf
│   │   ├── vpc.tf
│   │   ├── iam.tf
│   │   ├── ec2.tf
│   │   ├── ecs.tf
│   │   ├── s3.tf
│   │   ├── glue.tf
│   │   ├── cloudwatch.tf
│   │   └── variables.tf
│   └── user_data/
│       └── bootstrap.sh
├── docker/
│   ├── Dockerfile                  # §1.4
│   └── entrypoint.sh
├── dags/
│   └── s3_polars_etl.py            # §2.3
├── dbt/
│   ├── profiles.yml                # 템플릿
│   └── sample_project/
│       ├── dbt_project.yml
│       └── models/
│           └── .gitkeep
├── glue/
│   └── polars_etl.py               # stub
└── .github/
    └── workflows/
        ├── docker.yml              # 이미지 빌드 + ECR 푸시
        └── terraform.yml           # plan 자동, apply 수동 승인
```

---

## 7. 본 설계가 다루지 않는 것 (명시적 제외)

- **백업/복구 자동화** (사용자 요청으로 제외)
- Redshift 클러스터 / Serverless 정의 (Cosmos 설정 훅까지만)
- Fargate, NAT Gateway, VPC Endpoints
- ALB / HTTPS / SAML SSO
- Multi-AZ HA, 멀티 워커
- 사내 프록시 환경

이 항목들은 후속 sub-project로 분리해 별도 설계.
