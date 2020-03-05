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


ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
ENV HADOOP_HOME=/usr/local/hadoop
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/hadoop/bin:/usr/local/hive/bin
ENV HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
ENV HADOOP_SSH_OPTS="-p 22"

COPY sshd_config /etc/ssh/sshd_configs
RUN useradd -m -d /home/hadoop hadoop && echo 'hadoop:root' | chpasswd

RUN chown -R hadoop:hadoop /usr/local
RUN echo 'root:root' |chpasswd

EXPOSE 80

ENTRYPOINT ["/sbin/init"]
