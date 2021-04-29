FROM neo4j:4.2.3

ENV NEO4J_AUTH=neo4j/test

# Install findutils os package; used for housekeeping db backups
RUN apt update && apt install -y dos2unix findutils sudo python curl \
      && curl "https://bootstrap.pypa.io/pip/2.7/get-pip.py" | python && pip install j2cli

# Install ssh server
RUN apt install -y openssh-server openssh-client && mkdir /var/run/sshd && \
    passwd -u root && \
    ssh-keygen -A && \
    mkdir -p /root/.ssh/ && \
    sed -i 's|${exec_cmd} neo4j console|mkdir -p ~/.ssh \&\& env > ~/.ssh/environment \&\& chown -R root:root ~/.ssh \&\& service neo4j start \&\& /usr/sbin/sshd -D|' /docker-entrypoint.sh && \
    sed -i "s|#PermitUserEnvironment no|PermitUserEnvironment yes|" /etc/ssh/sshd_config && \
    sed -i "s|#UsePAM no|UsePAM yes|" /etc/ssh/sshd_config && \
    sed -i "s|#PasswordAuthentication yes|PasswordAuthentication no|" /etc/ssh/sshd_config

# Install unizp
RUN apt install -y unzip

# Copy public key for ansible deployment
COPY authorized_keys /root/.ssh/

# Add path to the bin directory
ENV PATH=${PATH}:/var/lib/neo4j/bin:/opt/neo4j-extension

# Something Shermane added
COPY neo4j /etc/init.d
