#!/usr/bin/env bash
PWD=`pwd`
HOST_MASTER_HADOOP_CONF_PATH="$PWD/master/hadoop/conf"
HOST_MASTER_SPARK_CONF_PATH="$PWD/master/spark/conf"
CONT_MASTER_HADOOP_CONF_PATH='/usr/local/hadoop/etc/hadoop'
HOST_HIVE_CONF_PATH="$PWD/hive/conf"
DAGS_FOLDER="$PWD/dags"

docker run --tmpfs /run -itd -v $HOST_MASTER_HADOOP_CONF_PATH:$CONT_MASTER_HADOOP_CONF_PATH \
-v $HOST_MASTER_SPARK_CONF_PATH:/usr/local/spark/conf \
-v $DAGS_FOLDER:/usr/local/dags \
-v /sys/fs/cgroup:/sys/fs/cgroup:ro \
-p 8088:8088 -p 57000:57000 -p 9001:9001 -p 50010:50010 -p 4040:4040 -p 8081:8081 -p 80:80 -p 18080:18080 \
--network=my-bridge-network \
--name=hadoop-master \
hadoop-master-img-centos7

docker run --tmpfs /run -itd -v $HOST_MASTER_HADOOP_CONF_PATH:$CONT_MASTER_HADOOP_CONF_PATH \
-v /sys/fs/cgroup:/sys/fs/cgroup:ro \
--network=my-bridge-network \
--name=hadoop-slave1 \
hadoop-slave-img-centos7

docker run --tmpfs /run -itd -v $HOST_MASTER_HADOOP_CONF_PATH:$CONT_MASTER_HADOOP_CONF_PATH \
-v /sys/fs/cgroup:/sys/fs/cgroup:ro \
--network=my-bridge-network \
--name=hadoop-slave2 \
hadoop-slave-img-centos7

docker run --tmpfs /run -itd -v $HOST_HIVE_CONF_PATH:/usr/local/hive/conf \
--network=my-bridge-network \
-v /sys/fs/cgroup:/sys/fs/cgroup:ro \
-p 9083:9083 -p 10002:10002 \
--name=hive-db \
hive-db-img-centos7

docker run --name mysql-hive -e MYSQL_ROOT_PASSWORD=root -d \
-p 3306:3306 \
--network=my-bridge-network mysql-for-hive-img
