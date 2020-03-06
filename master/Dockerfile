FROM centos:7

RUN yum -y update && yum clean all
RUN yum install wget which initscripts -y
RUN yum install java-1.8.0-openjdk-devel -y
RUN yum -y install openssh-server openssh-clients

WORKDIR /usr/local
RUN wget https://www-us.apache.org/dist/hadoop/common/hadoop-2.9.2/hadoop-2.9.2.tar.gz \
         && tar -xvf hadoop-2.9.2.tar.gz \
         && mv hadoop-2.9.2 hadoop \
         && rm -rf hadoop-2.9.2 \
         && rm hadoop-2.9.2.tar.gz

RUN wget http://us.mirrors.quenda.co/apache/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz \
         && tar -xvf spark-2.4.4-bin-hadoop2.7.tgz \
         && mv spark-2.4.4-bin-hadoop2.7 spark \
         && rm -rf spark-2.4.4-bin-hadoop2.7 \
         && rm spark-2.4.4-bin-hadoop2.7.tgz

RUN yum install python3 -y

RUN wget https://bootstrap.pypa.io/get-pip.py \
&& python3 get-pip.py

RUN pip install jupyter findspark
RUN pip install neo4j

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
ENV HADOOP_HOME=/usr/local/hadoop
ENV SPARK_HOME=/usr/local/spark
ENV PATH=/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/usr/local/hadoop/bin:/usr/local/spark/bin:$JAVA_HOME/bin:/usr/local/hive/bin
ENV HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root
ENV HADOOP_SSH_OPTS="-p 22"
ENV HIVE_CONF_DIR=/usr/local/hive/conf
ENV HIVE_HOME=/usr/local/hive
ENV PYSPARK_PYTHON=python3

COPY sshd_config /etc/ssh/sshd_configs

RUN mkdir spark/logs

RUN groupadd hadoop && \
useradd -m -d /home/hadoop -g hadoop hadoop && echo 'hadoop:root' | chpasswd && \
chgrp -R hadoop /usr/local && \
chmod -R g+rwx /usr/local && \
chgrp -R hadoop /tmp && \
chmod -R g+rwx /tmp

RUN mkdir airflow-dags && \
chgrp -R hadoop airflow-dags && \
chmod -R g+rwx airflow-dags

RUN echo 'root:root' |chpasswd

VOLUME [ "/sys/fs/cgroup"]
EXPOSE 80 18080

RUN yum install sshpass -y
COPY password.txt password.txt

ENTRYPOINT ["/sbin/init"]
