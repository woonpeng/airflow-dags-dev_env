FROM centos:7

RUN yum -y update && yum clean all
RUN yum install wget which initscripts -y
RUN yum install java-1.8.0-openjdk-devel -y
RUN yum -y install openssh-server openssh-clients

WORKDIR /usr/local

RUN wget https://www-us.apache.org/dist/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz \
    && tar -xvf apache-hive-3.1.2-bin.tar.gz \
    && mv apache-hive-3.1.2-bin hive \
    && rm -rf apache-hive-3.1.2-bin \
    && rm apache-hive-3.1.2-bin.tar.gz

RUN wget https://www-us.apache.org/dist/hadoop/common/hadoop-2.9.2/hadoop-2.9.2.tar.gz \
    && tar -xvf hadoop-2.9.2.tar.gz \
    && mv hadoop-2.9.2 hadoop \
    && rm -rf hadoop-2.9.2 \
    && rm hadoop-2.9.2.tar.gz

RUN wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-8.0.18-1.el8.noarch.rpm \
&& rpm -i mysql-connector-java-8.0.18-1.el8.noarch.rpm \
&& yum localinstall mysql-connector-java-8.0.18-1.el8.noarch.rpm

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
ENV HADOOP_HOME=/usr/local/hadoop
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/hadoop/bin:/usr/local/hive/bin
ENV HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
ENV HADOOP_SSH_OPTS="-p 22"
ENV HIVE_CONF_DIR=/usr/local/hive/conf
ENV HIVE_HOME=/usr/local/hive

EXPOSE 9083 10002 80

ENTRYPOINT ["/sbin/init"]
