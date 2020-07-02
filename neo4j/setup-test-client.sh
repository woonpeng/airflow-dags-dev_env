#!/usr/bin/env bash
docker run -itd --rm  --network my-bridge-network-dd --name neo4j-test-client alpine

# install openssh
docker exec neo4j-test-client apk add -U openssh

# copy over keys
docker exec neo4j-test-client mkdir /root/.ssh
docker cp ./secrets/id_rsa neo4j-test-client:/root/.ssh/
docker cp ./secrets/id_rsa.pub neo4j-test-client:/root/.ssh/

# need to set key permissions else ssh will reject
docker exec neo4j-test-client chmod  -R 600 /root/.ssh/

docker exec -it neo4j-test-client sh

