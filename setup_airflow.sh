#!/usr/bin/env bash
AIRFLOW_VERSION=2.0.1
PYTHON_VERSION="$(docker exec -it hadoop-master-dd python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
docker exec -it --user hadoop hadoop-master-dd python3 -m pip install --trusted-host files.pythonhosted.org --user "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

docker exec -it --user hadoop hadoop-master-dd /home/hadoop/.local/bin/airflow db init
docker exec -it --user hadoop hadoop-master-dd /home/hadoop/.local/bin/airflow users create --username admin --firstname Lim --lastname Peh --role Admin --email lim@peh.com --password "test"
#docker exec -it --user hadoop hadoop-master-dd /home/hadoop/.local/bin/airflow webserver --port 8086 -D
#docker exec -it --user hadoop hadoop-master-dd /home/hadoop/.local/bin/airflow scheduler -D
