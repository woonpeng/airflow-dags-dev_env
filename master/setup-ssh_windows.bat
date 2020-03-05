ECHO "creating ssh key for hadoop-master"
@ECHO OFF

docker exec -it --user hadoop hadoop-master bash -c "ssh-keygen -t rsa -b 4096 -f /home/hadoop/.ssh/id_rsa -q -N ''"
docker exec --user hadoop hadoop-master bash -c "cat /home/hadoop/.ssh/id_rsa.pub >> /home/hadoop/.ssh/authorized_keys"
docker exec --user hadoop hadoop-master bash -c "chmod og-wx /home/hadoop/.ssh/authorized_keys"
docker exec -it --user root hadoop-master bash -c "service sshd restart"

docker exec -it --user hadoop hadoop-master bash -c "sshpass -f 'password.txt' ssh-copy-id -o StrictHostKeyChecking=no hadoop@hadoop-slave1"
docker exec -it --user hadoop hadoop-master bash -c "sshpass -f 'password.txt' ssh-copy-id -o StrictHostKeyChecking=no hadoop@hadoop-slave2"
docker exec -it --user hadoop hadoop-master bash -c "sshpass -f 'password.txt' ssh-copy-id -o StrictHostKeyChecking=no hadoop@hadoop-master"

docker exec -it --user root hadoop-master bash -c "service sshd restart"
docker exec  --user hadoop hadoop-master /usr/local/hadoop/bin/hdfs namenode -format
docker exec -it --user hadoop hadoop-master /usr/local/hadoop/sbin/start-all.sh
docker exec -it --user hadoop hadoop-master hdfs dfs -mkdir /eventLogging
docker exec -it --user hadoop hadoop-master /usr/local/spark/sbin/start-history-server.sh
