from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, RenderConfig

RAW_BUCKET = os.environ.get("ACME_RAW_BUCKET", "acme-raw")
CURATED_BUCKET = os.environ.get("ACME_CURATED_BUCKET", "acme-curated")
GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME", "polars-etl-prd")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")

DBT_PROJECT_DIR = "/opt/airflow/dbt/sample_project"
DBT_PROFILES_PATH = "/opt/airflow/dbt/profiles.yml"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="s3_polars_etl",
    description="S3 raw -> Glue Python Shell (Polars) -> S3 curated -> dbt (skeleton)",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["polars", "glue", "dbt"],
) as dag:

    wait_raw = S3KeySensor(
        task_id="wait_raw",
        bucket_key=f"s3://{RAW_BUCKET}/dt={{{{ ds }}}}/_SUCCESS",
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 30,
        aws_conn_id=None,
    )

    run_glue = GlueJobOperator(
        task_id="run_glue_polars",
        job_name=GLUE_JOB_NAME,
        script_args={
            "--input": f"s3://{RAW_BUCKET}/dt={{{{ ds }}}}/",
            "--output": f"s3://{CURATED_BUCKET}/dt={{{{ ds }}}}/",
            "--run_id": "{{ run_id }}",
        },
        wait_for_completion=True,
        aws_conn_id=None,
        region_name=AWS_REGION,
    )

    verify_curated = S3KeySensor(
        task_id="verify_curated",
        bucket_key=f"s3://{CURATED_BUCKET}/dt={{{{ ds }}}}/_SUCCESS",
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 10,
        aws_conn_id=None,
    )

    dbt_models = DbtTaskGroup(
        group_id="dbt_models",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=ProfileConfig(
            profile_name="acme",
            target_name="dev",
            profiles_yml_filepath=DBT_PROFILES_PATH,
        ),
        render_config=RenderConfig(select=["tag:enabled"]),
    )

    wait_raw >> run_glue >> verify_curated >> dbt_models
