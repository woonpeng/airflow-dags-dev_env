echo "creating ssh key for hadoop-master-dd"
docker exec -it --user hadoop hadoop-master-dd bash -c 'ssh-keygen -t rsa -b 4096 -f /home/hadoop/.ssh/id_rsa -q -N ""'
docker exec --user hadoop hadoop-master-dd bash -c 'cat /home/hadoop/.ssh/id_rsa.pub >> /home/hadoop/.ssh/authorized_keys'
docker exec --user hadoop hadoop-master-dd bash -c 'chmod og-wx /home/hadoop/.ssh/authorized_keys'
docker exec -it --user hadoop hadoop-master-dd bash -c "service sshd restart"

docker exec -it --user hadoop hadoop-master-dd bash -c "sshpass -f "password.txt" ssh-copy-id -o StrictHostKeyChecking=no hadoop@hadoop-slave1-dd"
docker exec -it --user hadoop hadoop-master-dd bash -c "sshpass -f "password.txt" ssh-copy-id -o StrictHostKeyChecking=no hadoop@hadoop-slave2-dd"
docker exec -it --user hadoop hadoop-master-dd bash -c "sshpass -f "password.txt" ssh-copy-id -o StrictHostKeyChecking=no hadoop@hadoop-master-dd"

docker exec -it --user hadoop hadoop-master-dd bash -c "service sshd restart"
docker exec --user hadoop hadoop-master-dd /usr/local/hadoop/bin/hdfs namenode -format
docker exec -it --user hadoop hadoop-master-dd /usr/local/hadoop/sbin/start-all.sh
docker exec -it --user hadoop hadoop-master-dd hdfs dfs -mkdir /eventLogging
docker exec -it --user hadoop hadoop-master-dd /usr/local/spark/sbin/start-history-server.sh

