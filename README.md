# Hadoop, Spark, Hive
This project contains Hadoop, Spark, Hive, and Neo4J

# Setup
Build all docker images in the database, master and slave folder
(It will take awhile ...)

Build sql image
```
cd mysql
docker build -t mysql-for-hive-img .
```

Build Hadoop master image (contains spark too)
```
cd master
docker build -t hadoop-master-img-centos7-dd .
```

Build Hadoop slave image
```
cd slave
docker build -t hadoop-slave-img-centos7 .
```

Build custom Neo4j image
```
cd neo4j
docker build -t duediligence-neo4j .
```

Create the bridge network
```
docker network create -d bridge my-bridge-network-dd
```

Then run in root project folder (read below if you are using Windows)
```
./setup.sh
```

~~For Windows, run `./setup_windows.bat` on a bash shell:~~

If you are using a Windows machine, and have Git Bash installed, follow this workaround to mitigate the path interpretation issues: https://github.com/borekb/docker-path-workaround .

Answer `y` when you are prompted to remove kernel spec.

Also, if you face `org.freedesktop.PolicyKit1 was not provided by any .service files` errors you may have to reinstall `polkit` on `hadoop-master-dd`. Do this:

```
docker exec -it --user root bash
yum reinstall polkit
```

Please refer to the scripts (database/master folders) on what it does as I am too lazy to write out.

# How do I know if I setup correctly?

Go to http://localhost:8088, you will be able to see the hadoop web page with 2 active nodes!

Try running a spark submit command, go to localhost:8080 and you can see the active jobs!

```
docker exec -it hadoop-master-dd /bin/bash
cd spark
./bin/spark-submit --class org.apache.spark.examples.SparkPi \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 1 \
    examples/jars/spark-examples*.jar \
    10
```

Check if you can access the hive table

```
docker exec -it hadoop-master-dd /bin/bash
spark/bin/spark-shell
val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
sqlContext.sql("show databases").show()
```

To start your Jupyter notebook, run:

```
docker exec -it --user hadoop hadoop-master-dd jupyter notebook --ip=0.0.0.0 --port=8082
```

If your containers have stopped, use:

```
./startup.sh
```

Yay!
