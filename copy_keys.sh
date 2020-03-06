mkdir -p ~/hmaster-dd_keys
docker cp hadoop-master-dd:/home/hadoop/.ssh/id_rsa ~/hmaster-dd_keys/id_rsa
docker cp hadoop-master-dd:/home/hadoop/.ssh/id_rsa.pub ~/hmaster-dd_keys/id_rsa.pub
