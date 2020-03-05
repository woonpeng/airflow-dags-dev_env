docker start hadoop-master
docker start hadoop-slave1
docker start hadoop-slave2
docker start hive-db
docker start mysql-hive

docker exec -it hadoop-master bash -c "service sshd restart"
docker exec -it mysql-hive bash -c "service mysqld restart"

docker exec -d hive-db hive --service metastore
docker exec -it --user hadoop hadoop-master bash -c "/usr/local/hadoop/sbin/start-all.sh"
