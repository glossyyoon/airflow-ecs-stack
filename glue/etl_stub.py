"""Glue Python Shell stub — proves the Airflow → Glue round-trip.

Scope:
  Verify that Airflow's GlueJobOperator can start a Glue job, the job
  picks up its script args, talks to S3 with the attached job role, and
  reports success back to Airflow. Does NOT perform any real ETL.

Args (passed by Airflow GlueJobOperator.script_args):
  --input   s3://<raw>/dt=YYYY-MM-DD/
  --output  s3://<curated>/dt=YYYY-MM-DD/
  --run_id  Airflow run id (for log correlation)

Behaviour:
  1. List objects under --input and print the count
  2. Write a tiny summary.txt + _SUCCESS marker under --output

Dependencies: stdlib + boto3 (already in Glue Python Shell). No extra
python modules installed at runtime, so this works on Glue 3.0 / Python 3.9
without --additional-python-modules.
"""
from __future__ import annotations

import argparse
import sys
from urllib.parse import urlparse

import boto3


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--run_id", required=True)
    return p.parse_args(argv)


def count_input_objects(s3_uri: str) -> int:
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    total = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        total += len(page.get("Contents", []))
    return total


def write_outputs(output_uri: str, run_id: str, source_count: int) -> None:
    parsed = urlparse(output_uri)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/").rstrip("/")
    s3 = boto3.client("s3")

    summary = (
        f"run_id={run_id}\n"
        f"source_object_count={source_count}\n"
        f"glue_runtime=python-shell-3.9\n"
    ).encode()
    s3.put_object(Bucket=bucket, Key=f"{prefix}/summary.txt", Body=summary)
    s3.put_object(Bucket=bucket, Key=f"{prefix}/_SUCCESS", Body=b"")


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    print(f"[glue-stub] run_id={args.run_id}", flush=True)
    print(f"[glue-stub] input ={args.input}", flush=True)
    print(f"[glue-stub] output={args.output}", flush=True)

    source_count = count_input_objects(args.input)
    print(f"[glue-stub] source objects under input: {source_count}", flush=True)

    write_outputs(args.output, args.run_id, source_count)
    print(f"[glue-stub] wrote summary.txt + _SUCCESS under {args.output}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
