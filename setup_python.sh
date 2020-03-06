#!/usr/bin/env bash
# create Python 3 environment
docker exec -it hadoop-master-dd python3 -m pip install -r airflow-dags/requirements.txt
docker exec -it --user hadoop hadoop-master-dd python3 -m ipykernel install --user --name airflow-dags_py3 --display-name "airflow-dags (Python 3)"

# installing ipykernel installs a wrong "python3" kernel
docker exec -it --user hadoop hadoop-master-dd jupyter kernelspec uninstall python3

