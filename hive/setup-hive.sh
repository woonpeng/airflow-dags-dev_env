docker exec -it hive-db bash -c "cp /usr/share/java/mysql-connector-java.jar /usr/local/hive/lib"

# create hive database directories
docker exec hive-db hadoop/bin/hdfs dfs -mkdir user
docker exec hive-db hadoop/bin/hdfs dfs -mkdir user/hive
docker exec hive-db hadoop/bin/hdfs dfs -mkdir user/hive/warehouse
docker exec hive-db hadoop/bin/hdfs dfs -mkdir tmp
docker exec hive-db hadoop/bin/hdfs dfs -chmod -R a+rwx user/hive/warehouse
docker exec hive-db hadoop/bin/hdfs dfs -chmod g+w tmp

docker exec -d hive-db hive --service metastore &
echo "hive setup done!"
