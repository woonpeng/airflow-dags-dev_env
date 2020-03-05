mkdir -p ~/hmaster_keys
docker cp hadoop-master:/home/hadoop/.ssh/id_rsa ~/hmaster_keys/id_rsa
docker cp hadoop-master:/home/hadoop/.ssh/id_rsa.pub ~/hmaster_keys/id_rsa.pub
