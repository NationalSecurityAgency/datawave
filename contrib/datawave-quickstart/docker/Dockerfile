FROM maven:3-amazoncorretto-11 AS builder
FROM rockylinux/rockylinux:8

ENV MAVEN_HOME=/usr/share/maven
COPY --from=builder /usr/share/maven /usr/share/maven
RUN ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

ARG DATAWAVE_COMMIT_ID
ARG DATAWAVE_BRANCH_NAME
ARG DATAWAVE_JAVA_HOME
ARG DATAWAVE_BUILD_PROFILE
ARG DATAWAVE_INGEST_TEST_DATA_SKIP
ARG DATAWAVE_MAVEN_REPO="https://maven.pkg.github.com/NationalSecurityAgency/datawave"

ARG ACCUMULO_URL
ARG HADOOP_URL
ARG WILDFLY_URL
ARG ZOOKEEPER_URL

ARG ACCUMULO_VERSION
ARG HADOOP_VERSION
ARG WILDFLY_VERSION
ARG ZOOKEEPER_VERSION

ARG ACCUMULO_DIST_SHA512_CHECKSUM
ARG HADOOP_DIST_SHA512_CHECKSUM
ARG WILDFLY_DIST_SHA512_CHECKSUM
ARG ZOOKEEPER_DIST_SHA512_CHECKSUM

USER root

ENV USER=root
ENV HADOOP_IDENT_STRING=root
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root

ENV DW_ACCUMULO_DIST_URI=$ACCUMULO_URL
ENV DW_HADOOP_DIST_URI=$HADOOP_URL
ENV DW_WILDFLY_DIST_URI=$WILDFLY_URL
ENV DW_ZOOKEEPER_DIST_URI=$ZOOKEEPER_URL

ENV DW_ACCUMULO_VERSION=$ACCUMULO_VERSION
ENV DW_HADOOP_VERSION=$HADOOP_VERSION
ENV DW_WILDFLY_VERSION=$WILDFLY_VERSION
ENV DW_ZOOKEEPER_VERSION=$ZOOKEEPER_VERSION

ENV DW_ACCUMULO_DIST_SHA512_CHECKSUM=$ACCUMULO_DIST_SHA512_CHECKSUM
ENV DW_HADOOP_DIST_SHA512_CHECKSUM=$HADOOP_DIST_SHA512_CHECKSUM
ENV DW_WILDFLY_DIST_SHA512_CHECKSUM=$WILDFLY_DIST_SHA512_CHECKSUM
ENV DW_ZOOKEEPER_DIST_SHA512_CHECKSUM=$ZOOKEEPER_DIST_SHA512_CHECKSUM

ENV DW_DATAWAVE_BUILD_PROFILE=$DATAWAVE_BUILD_PROFILE
ENV DW_DATAWAVE_INGEST_TEST_DATA_SKIP=$DATAWAVE_INGEST_TEST_DATA_SKIP
ENV DW_MAVEN_REPOSITORY=$DATAWAVE_MAVEN_REPO
ENV DW_WGET_OPTS="-q --no-check-certificate"
ENV JAVA_HOME=$DATAWAVE_JAVA_HOME
ENV PATH=$JAVA_HOME/bin:$PATH

# Bind services to all interfaces
ENV DW_BIND_HOST=0.0.0.0

# Bind accumulo specifically to localhost
# This can be overridden at runtime to match the service name using DW_CONTAINER_HOST
ENV DW_ACCUMULO_BIND_HOST=localhost

# Build context should be the DataWave source root, minus .git and other dirs. See .jkube-dockerignore

COPY . /opt/datawave

RUN dnf -y update
RUN dnf -y install sudo findutils
RUN find /opt/datawave -type d -exec chmod 0750 {} \;
RUN find /opt/datawave -type f -exec chmod 0640 {} \;
RUN find /opt/datawave -name "*.sh" -type f -exec chmod 0750 {} \;

# Install dependencies, configure password-less/zero-prompt SSH...
RUN dnf -y install gcc-c++ openssl python3 openssh openssh-server openssh-clients openssl-libs java-11-openjdk-devel which bc wget git iproute && \
    /bin/bash -c "source /opt/datawave/contrib/datawave-quickstart/bin/env.sh && jdkIsConfigured" && \
    echo "source /opt/datawave/contrib/datawave-quickstart/bin/env.sh" >> ~/.bashrc && \
    dnf clean all && \
    ssh-keygen -q -N "" -t rsa -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys && \
    ssh-keygen -q -N "" -t dsa -f /etc/ssh/ssh_host_dsa_key && \
    ssh-keygen -q -N "" -t rsa -f /etc/ssh/ssh_host_rsa_key && \
    ssh-keygen -q -N "" -t ecdsa -f /etc/ssh/ssh_host_ecdsa_key && \
    ssh-keygen -q -N "" -t ed25519 -f /etc/ssh/ssh_host_ed25519_key && \
    echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config && \
    echo "UserKnownHostsFile /dev/null" >> /etc/ssh/ssh_config && \
    echo "LogLevel QUIET" >> /etc/ssh/ssh_config


RUN which java && which javac && java -version && javac -version && \
    which mvn && echo $(mvn --version)

WORKDIR /opt/datawave

# Create new Git repo and configure environment...

RUN git init && \
    git add . && \
    git config user.email "root@localhost.local" && \
    git config user.name "Root User" && \
    git commit -m "Source Branch :: $DATAWAVE_BRANCH_NAME :: Source Commit :: $DATAWAVE_COMMIT_ID"

# This works exactly like the setup for a non-containerized instance of the datawave-quickstart
# environment. That is, ~/.bashrc and datawave-quickstart/bin/env.sh are sourced, bootstrapping
# the quickstart environment. Likewise, 'allInstall' and 'datawaveStart' wrapper functions are
# used to initialize services and their log dirs. Log dirs are cleaned up after the install.

RUN /bin/bash -c "/usr/bin/nohup /usr/sbin/sshd -D &" && \
    /bin/bash -c "source ~/.bashrc && allInstall" && \
    rm -rf contrib/datawave-quickstart/datawave-ingest/logs/* && \
    rm -rf contrib/datawave-quickstart/hadoop/logs/* && \
    rm -rf contrib/datawave-quickstart/accumulo/logs/* && \
    rm -rf contrib/datawave-quickstart/wildfly/standalone/log/*

# Lastly, establish volumes for data, logs & other directories, wire up
# the entrypoint & bootstrap scripts, expose ports, and set default CMD...

# Primary data volume (for HDFS, Accumulo, ZooKeeper, etc)
VOLUME ["/opt/datawave/contrib/datawave-quickstart/data"]

# In case user wants to rebuild DW
VOLUME ["$HOME/.m2/repository"]

VOLUME ["/opt/datawave/contrib/datawave-quickstart/hadoop/logs"]
VOLUME ["/opt/datawave/contrib/datawave-quickstart/accumulo/logs"]
VOLUME ["/opt/datawave/contrib/datawave-quickstart/wildfly/standalone/log"]
VOLUME ["/opt/datawave/contrib/datawave-quickstart/datawave-ingest/logs"]

EXPOSE 8443 9995 9870 8088 9000 2181

WORKDIR /opt/datawave/contrib/datawave-quickstart

RUN ln -s /opt/datawave/contrib/datawave-quickstart/docker/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh && \
    ln -s /opt/datawave/contrib/datawave-quickstart/docker/datawave-bootstrap.sh /usr/local/bin/datawave-bootstrap.sh

# The entrypoint script will ensure that sshd is started, and it'll simply 'exec "$@"' whatever command is passed

ENTRYPOINT ["docker-entrypoint.sh"]

# Default CMD uses the bootstrap script to start up DataWave's web services. Due to the --bash flag, it'll
# exec /bin/bash for the container process, intended for 'docker run -it ...' usage.

CMD ["datawave-bootstrap.sh", "--web", "--bash"]

# Without the --bash flag, datawave-bootstrap.sh will go into an infinite loop to prevent the container
# from exiting, better for 'docker run -d ...' usage
