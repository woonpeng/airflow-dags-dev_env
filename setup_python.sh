#!/usr/bin/env bash
# create Python 3 environment
docker exec -it hadoop-master-dd python3 -m pip install --upgrade pip

requirements="$(cat ./python_requirements.txt)"
docker exec -it hadoop-master-dd python3 -m pip install ${requirements/$'\n'/' '}
docker exec -it --user hadoop hadoop-master-dd python3 -m ipykernel install --user --name airflow-dags_py3 --display-name "airflow-dags (Python 3)"

# installing ipykernel installs a wrong "python3" kernel
docker exec -it --user hadoop hadoop-master-dd yes | jupyter kernelspec uninstall -y python3
