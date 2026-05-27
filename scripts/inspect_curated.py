"""Print the contents of the smoke-test Glue job's output.

Usage:
    python scripts/inspect_curated.py \
        --bucket airflow-ecs-curated-857988933565 \
        --date 2026-05-27

Expected after a successful DAG run:
    dt=YYYY-MM-DD/summary.txt   (small text, written by glue/etl_stub.py)
    dt=YYYY-MM-DD/_SUCCESS      (empty marker)
"""
from __future__ import annotations

import argparse
import sys

import boto3


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", required=True)
    p.add_argument("--date", required=True)
    p.add_argument("--region", default="ap-northeast-2")
    args = p.parse_args(argv)

    s3 = boto3.client("s3", region_name=args.region)
    prefix = f"dt={args.date}/"
    listing = s3.list_objects_v2(Bucket=args.bucket, Prefix=prefix).get("Contents", [])

    print(f"objects under s3://{args.bucket}/{prefix}:")
    for obj in listing:
        print(f"  {obj['Key']:60s} {obj['Size']:>10d}B")

    summary_keys = [o["Key"] for o in listing if o["Key"].endswith("summary.txt")]
    if not summary_keys:
        print("no summary.txt found", file=sys.stderr)
        return 1

    body = s3.get_object(Bucket=args.bucket, Key=summary_keys[0])["Body"].read().decode()
    print()
    print("summary.txt:")
    for line in body.splitlines():
        print(f"  {line}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
