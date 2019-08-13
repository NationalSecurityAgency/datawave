FROM openjdk:8-jdk-alpine

ARG DATAWAVE_COMMIT_ID
ARG DATAWAVE_BRANCH_NAME
ARG DATAWAVE_JAVA_HOME

USER root

# ENV params for hadoop here are a hack/fix for the "chown: missing operand after '**/hadoop/logs'"
# errors thrown by hadoop startup scripts for both the resourcemanager and historyserver. Those
# errors occur due to $USER being mysteriously null/undefined at the point that log file names are
# established and subsequently chowned in those scripts. But aside from the chown errors and a few
# irregularly-named log files, the null $USER issue doesn't seem to have any other negative impact.
# Mostly an annoyance and odd that it only seems to occur in Docker, which is why I'm documenting
# it here (issue has been observed w/ both centos6 and centos7 base images)

ENV YARN_IDENT_STRING=root HADOOP_MAPRED_IDENT_STRING=root

# Build context should be the DataWave source root, minus .git and other dirs. See .dockerignore

COPY . /opt/datawave

# Install dependencies, configure password-less/zero-prompt SSH...

RUN apk --no-cache add --upgrade openssl openssh which bc wget git bash procps findutils curl gettext && \
    ssh-keygen -q -N "" -t rsa -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys && \
    ssh-keygen -q -N "" -t dsa -f /etc/ssh/ssh_host_dsa_key && \
    ssh-keygen -q -N "" -t rsa -f /etc/ssh/ssh_host_rsa_key && \
    ssh-keygen -q -N "" -t ecdsa -f /etc/ssh/ssh_host_ecdsa_key && \
    ssh-keygen -q -N "" -t ed25519 -f /etc/ssh/ssh_host_ed25519_key && \
    echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config && \
    echo "UserKnownHostsFile /dev/null" >> /etc/ssh/ssh_config && \
    echo "LogLevel QUIET" >> /etc/ssh/ssh_config && \
    sed -i s/#PermitRootLogin.*/PermitRootLogin\ yes/ /etc/ssh/sshd_config && \
    sed -i s/root:!/"root:*"/g /etc/shadow

SHELL ["/bin/bash", "-c"]

WORKDIR /opt/datawave

# Create new Git repo and configure environment...

RUN rm -f .dockerignore && \
    git init && \
    git add . && \
    git config user.email "root@localhost.local" && \
    git config user.name "Root User" && \
    git commit -m "Source Branch :: $DATAWAVE_BRANCH_NAME :: Source Commit :: $DATAWAVE_COMMIT_ID" && \
    echo "export DW_ACCUMULO_DIST_URI=file://$(find /opt/datawave/contrib/datawave-quickstart/bin/services/accumulo -name 'accumulo*.tar.gz')" >> ~/.bashrc && \
    echo "export DW_ZOOKEEPER_DIST_URI=file://$(find /opt/datawave/contrib/datawave-quickstart/bin/services/accumulo -name 'zookeeper*.tar.gz')" >> ~/.bashrc && \
    echo "export DW_MAVEN_DIST_URI=file://$(find /opt/datawave/contrib/datawave-quickstart/bin/services/maven -name 'apache-maven*.tar.gz')" >> ~/.bashrc && \
    echo "export DW_HADOOP_DIST_URI=file://$(find /opt/datawave/contrib/datawave-quickstart/bin/services/hadoop -name 'hadoop*.tar.gz')" >> ~/.bashrc && \
    echo "export DW_WILDFLY_DIST_URI=file://$(find /opt/datawave/contrib/datawave-quickstart/bin/services/datawave -name 'wildfly*.tar.gz')" >> ~/.bashrc && \
    echo "export JAVA_HOME=\"$DATAWAVE_JAVA_HOME\"" >> ~/.bashrc && \
    echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> ~/.bashrc && \
    echo "source /opt/datawave/contrib/datawave-quickstart/bin/env.sh" >> ~/.bashrc

# This works exactly like the setup for a non-containerized instance of the datawave-quickstart
# environment. That is, ~/.bashrc and datawave-quickstart/bin/env.sh are sourced, bootstrapping
# the quickstart environment. Likewise, 'allInstall' and 'datawaveStart' wrapper functions are
# used to initialize services and their log dirs. Finally, web services are tested, services are
# stopped gracefully, and any cruft is purged.

RUN /bin/bash -c "/usr/bin/nohup /usr/sbin/sshd -D &" && \
    /bin/bash -c "source ~/.bashrc && allInstall && datawaveStart && datawaveWebTest && allStop" && \
    echo "0.0.0.0" > contrib/datawave-quickstart/accumulo/conf/monitor && \
    rm -rf contrib/datawave-quickstart/datawave-ingest/logs/* && \
    rm -rf contrib/datawave-quickstart/hadoop/logs/* && \
    rm -rf contrib/datawave-quickstart/accumulo/logs/* && \
    rm -rf contrib/datawave-quickstart/wildfly/standalone/log/*

# Lastly, establish volumes for data, logs & other directories, wire up
# the entrypoint & bootstrap scripts, expose ports, and set default CMD...

# Primary data volume (for HDFS, Accumulo, ZooKeeper, etc)
VOLUME ["/opt/datawave/contrib/datawave-quickstart/data"]

# In case user wants to rebuild DW
VOLUME ["~/.m2/repository"]

VOLUME ["/opt/datawave/contrib/datawave-quickstart/hadoop/logs"]
VOLUME ["/opt/datawave/contrib/datawave-quickstart/accumulo/logs"]
VOLUME ["/opt/datawave/contrib/datawave-quickstart/wildfly/standalone/log"]
VOLUME ["/opt/datawave/contrib/datawave-quickstart/datawave-ingest/logs"]

EXPOSE 8443 9995 50070 8088 9000 2181

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
