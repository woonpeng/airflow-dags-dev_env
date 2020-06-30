#!/usr/bin/env bash
mkdir -p ./secrets
docker cp hadoop-master-dd:/home/hadoop/.ssh/id_rsa ./secrets/id_rsa
docker cp hadoop-master-dd:/home/hadoop/.ssh/id_rsa.pub ./secrets/id_rsa.pub

# adds modified key to neo4j_keys folder
mkdir -p ./secrets/neo4j_keys
./neo4j/add_ssh_key.sh -n 1 -f ./secrets/neo4j_keys/authorized_keys -r ./secrets/id_rsa.pub

# copy keys into neo4j container (volume mount does not work for keys cuz of permission issues?)
docker cp ./secrets/neo4j_keys/authorized_keys neo4j:/root/.ssh/authorized_keys
