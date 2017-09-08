FROM centos:centos6

ARG DATAWAVE_COMMIT_ID
ARG DATAWAVE_BRANCH_NAME

USER root

# Context should be DataWave source root dir, minus .git and other dirs. See .dockerignore

COPY . /opt/datawave

# Install dependencies, configure password-less/zero-prompt SSH

RUN yum -y install openssh openssh-server openssh-clients openssl-libs bc wget git && \
    chkconfig sshd on && \
    yum clean all && \
    ssh-keygen -q -N "" -t rsa -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys && \
    echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config && \
    echo "UserKnownHostsFile /dev/null" >> /etc/ssh/ssh_config && \
    echo "LogLevel QUIET" >> /etc/ssh/ssh_config

WORKDIR /opt/datawave

# Create new Git repo for convenience

RUN rm -f .dockerignore && \
    git init && \
    git add . && \
    git commit -m "Source Branch :: $DATAWAVE_BRANCH_NAME :: Source Commit :: $DATAWAVE_COMMIT_ID"

# This works exactly like the setup for a non-containerized version of the quickstart
# environment. That is, ~/.bashrc (and thus datawave-quickstart/bin/env.sh) is sourced, which
# bootstraps the quickstart environment. Likewise, 'installAll' and 'datawaveStart' are used
# to initialize services. Finally, services are stopped gracefully and any cruft is purged

RUN /sbin/service sshd start && \
    echo "source /opt/datawave/contrib/datawave-quickstart/bin/env.sh" >> ~/.bashrc && \
    /bin/bash -c "source ~/.bashrc && installAll && datawaveStart && datawaveWebTest && stopAll" && \
    rm -rf contrib/datawave-quickstart/datawave-ingest/logs/* && \
    rm -rf contrib/datawave-quickstart/hadoop/logs/* && \
    rm -rf contrib/datawave-quickstart/accumulo/logs/* && \
    rm -rf contrib/datawave-quickstart/wildfly/standalone/log/*

# Lastly, establish volumes for data, logs & other directories, wire up
# the entrypoint & bootstrap scripts, expose ports, and set default CMD...

# Data volume (HDFS, ZooKeeper, etc)
VOLUME ["/opt/datawave/contrib/datawave-quickstart/data"]

# In case user wants to rebuild DW
VOLUME ["~/.m2/repository"]

# Logs
VOLUME ["/opt/datawave/contrib/datawave-quickstart/hadoop/logs"]
VOLUME ["/opt/datawave/contrib/datawave-quickstart/accumulo/logs"]
VOLUME ["/opt/datawave/contrib/datawave-quickstart/wildfly/standalone/log"]
VOLUME ["/opt/datawave/contrib/datawave-quickstart/datawave-ingest/logs"]

EXPOSE 8443 9995 50070 8088 9000 22

WORKDIR /opt/datawave/contrib/datawave-quickstart

RUN ln -s /opt/datawave/contrib/datawave-quickstart/docker/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh && \
    ln -s /opt/datawave/contrib/datawave-quickstart/docker/datawave-bootstrap.sh /usr/local/bin/datawave-bootstrap.sh

# The entrypoint script will ensure that the sshd service is started, etc, and it'll
# simply 'exec "$@"' whatever command is passed

ENTRYPOINT ["docker-entrypoint.sh"]

# Default CMD uses the bootstrap script to start up DataWave's web services. Due to the --bash arg it'll
# exec /bin/bash for the container process, which is appropriate for 'docker run -it ...' usage.

CMD ["datawave-bootstrap.sh", "--web", "--bash"]

# Note that without the --bash arg, datawave-bootstrap.sh will exec an infinite loop to prevent the container
# from exiting, which is more appropriate for 'docker run -d ...' usage
