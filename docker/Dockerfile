FROM docker.io/apache/airflow:3.2.1-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential \
      libpq-dev \
      unzip \
      libaio1 \
      wget \
      libffi-dev \
      libssl-dev \
      git \
      tini \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

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

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__EXECUTOR=LocalExecutor \
    AWS_DEFAULT_REGION=ap-northeast-2

COPY --chown=airflow:0 entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/usr/bin/tini", "--", "/entrypoint.sh"]
