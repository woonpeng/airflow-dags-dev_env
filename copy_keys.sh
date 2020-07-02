#!/usr/bin/env bash
mkdir -p ./secrets
docker cp hadoop-master-dd:/home/hadoop/.ssh/id_rsa ./secrets/id_rsa
docker cp hadoop-master-dd:/home/hadoop/.ssh/id_rsa.pub ./secrets/id_rsa.pub

# adds modified key to neo4j_keys folder
mkdir -p ./secrets/neo4j_keys
./neo4j/add_ssh_key.sh -n 1 -f ./secrets/neo4j_keys/authorized_keys -r ./secrets/id_rsa.pub

# copy keys into neo4j container (volume mount does not work for keys cuz of permission issues?)
docker cp ./secrets/neo4j_keys/authorized_keys neo4j:/root/.ssh/authorized_keys

# clear neo4j host from hadoop-master-dd
docker exec hadoop-master-dd rm -rf /home/hadoop/.ssh/known_hosts
docker exec -t --user hadoop hadoop-master-dd ssh -o StrictHostKeyChecking=no root@neo4j
echo "Don't worry about 'Access Denied', that is the expected behavior since we are running a forbidden command just to add the host key"
