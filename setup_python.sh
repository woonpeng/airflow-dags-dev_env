# create finnet Python 2 environment
docker exec -it hadoop-master python2 get-pip.py
docker exec -it hadoop-master python2 -m pip install -r dags/requirements.txt
docker exec -it --user hadoop hadoop-master python2 -m ipykernel install --user --name finnet-pipeline --display-name "finnet-pipeline (Python 2)"

docker exec -it hadoop-master python3 -m pip install -r dags/requirements_py3.txt
docker exec -it --user hadoop hadoop-master python3 -m ipykernel install --user --name finnet-pipeline_py3 --display-name "finnet-pipeline (Python 3)"

# installing ipykernel installs a wrong "python2" kernel
docker exec -it hadoop-master jupyter kernelspec uninstall python2
docker exec -it hadoop-master jupyter kernelspec uninstall python3

