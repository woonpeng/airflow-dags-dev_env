#!/usr/bin/env bash
docker run -itd --rm  --network my-bridge-network-dd --name neo4j-test-client alpine
docker exec neo4j-test-client apk add -U openssh
docker exec neo4j-test-client mkdir /root/.ssh
docker cp ./secrets/id_rsa neo4j-test-client:/root/.ssh/
docker exec neo4j-test-client chmod 600 /root/.ssh/id_rsa
docker exec -it neo4j-test-client sh

