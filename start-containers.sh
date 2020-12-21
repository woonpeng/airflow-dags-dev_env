#!/usr/bin/env bash
PWD=`pwd`
HOST_MASTER_HADOOP_CONF_PATH="$PWD/master/hadoop/conf"
HOST_MASTER_SPARK_CONF_PATH="$PWD/master/spark/conf"
CONT_MASTER_HADOOP_CONF_PATH='/usr/local/hadoop/etc/hadoop'
HOST_HIVE_CONF_PATH="$PWD/hive/conf"
REPO_FOLDER="$(dirname "$PWD")/airflow-dags"

docker run --tmpfs /run -itd -v $HOST_MASTER_HADOOP_CONF_PATH:$CONT_MASTER_HADOOP_CONF_PATH \
-v $HOST_MASTER_SPARK_CONF_PATH:/usr/local/spark/conf \
-v $REPO_FOLDER:/usr/local/airflow-dags \
-v /sys/fs/cgroup:/sys/fs/cgroup:ro \
-p 8088:8088 -p 57000:57000 -p 9009:9009 -p 50010:50010 -p 4040:4040 -p 8082:8082 -p 80:80 -p 18080:18080 \
--network=my-bridge-network-dd \
--name=hadoop-master-dd \
hadoop-master-img-centos7-dd

docker run --tmpfs /run -itd -v $HOST_MASTER_HADOOP_CONF_PATH:$CONT_MASTER_HADOOP_CONF_PATH \
-v /sys/fs/cgroup:/sys/fs/cgroup:ro \
--network=my-bridge-network-dd \
--name=hadoop-slave1-dd \
hadoop-slave-img-centos7

docker run --tmpfs /run -itd -v $HOST_MASTER_HADOOP_CONF_PATH:$CONT_MASTER_HADOOP_CONF_PATH \
-v /sys/fs/cgroup:/sys/fs/cgroup:ro \
--network=my-bridge-network-dd \
--name=hadoop-slave2-dd \
hadoop-slave-img-centos7

#docker run --tmpfs /run -itd -v $HOST_HIVE_CONF_PATH:/usr/local/hive/conf \
#--network=my-bridge-network-dd \
#-v /sys/fs/cgroup:/sys/fs/cgroup:ro \
#-p 9083:9083 -p 10002:10002 \
#--name=hive-db-dd \
#hive-db-img-centos7

docker run --name mysql-hive-dd -e MYSQL_ROOT_PASSWORD=root -d \
-p 3306:3306 \
--network=my-bridge-network-dd mysql-for-hive-img

docker run -itd -p 7474:7474 -p 7687:7687 --network=my-bridge-network-dd --name=neo4j duediligence-neo4j