"""Glue Python Shell 5.0 entry — Polars based ETL stub.

Args (passed by Airflow GlueJobOperator.script_args):
  --input   s3://<raw>/dt=YYYY-MM-DD/
  --output  s3://<curated>/dt=YYYY-MM-DD/
  --run_id  Airflow run id (for log correlation)

Behaviour:
  1. Read all parquet files under --input as a LazyFrame.
  2. Run a trivial transform (placeholder — real logic plugs in here).
  3. Write the result back as a single partitioned parquet file under --output.
  4. Write a 0-byte _SUCCESS marker so the downstream S3KeySensor passes.
"""
from __future__ import annotations

import argparse
import sys
from urllib.parse import urlparse

import boto3
import polars as pl


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--run_id", required=True)
    return parser.parse_args(argv)


def write_success_marker(s3_uri: str) -> None:
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/").rstrip("/") + "/_SUCCESS"
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=b"")


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    print(f"[polars-etl] run_id={args.run_id} input={args.input} output={args.output}", flush=True)

    df = pl.scan_parquet(args.input + "*.parquet")
    transformed = df  # plug real transformations here
    out_file = args.output.rstrip("/") + "/part-0000.parquet"
    transformed.sink_parquet(out_file)
    write_success_marker(args.output)

    print(f"[polars-etl] wrote {out_file} and _SUCCESS", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
