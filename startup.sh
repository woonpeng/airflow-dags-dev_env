docker start hadoop-master-dd
docker start hadoop-slave1-dd
docker start hadoop-slave2-dd
docker start hive-db-dd
docker start mysql-hive-dd

docker exec -it hadoop-master-dd bash -c "service sshd restart"
docker exec -it mysql-hive-dd bash -c "service mysqld restart"

docker exec -d hive-db-dd hive --service metastore
docker exec -it --user hadoop hadoop-master-dd bash -c "/usr/local/hadoop/sbin/start-all.sh"
docker exec --user hadoop hadoop-master-dd hadoop/bin/hdfs dfs -mkdir /user
docker exec --user hadoop hadoop-master-dd hadoop/bin/hdfs dfs -mkdir /user/hive
docker exec --user hadoop hadoop-master-dd hadoop/bin/hdfs dfs -mkdir /user/hive/warehouse
