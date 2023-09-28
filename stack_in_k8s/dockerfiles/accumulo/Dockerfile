

ARG BUILDER_IMAGE_NAME=centos
ARG BUILDER_IMAGE_TAG=7

ARG BASE_IMAGE_NAME=centos
ARG BASE_IMAGE_TAG=7

ARG ACCUMULO_VERSION=1.10.2
ARG HADOOP_VERSION=3.3.3
ARG ZOOKEEPER_VERSION=3.6.3

FROM ${BUILDER_IMAGE_NAME}:${BUILDER_IMAGE_TAG} as builder

ARG ACCUMULO_VERSION
ARG HADOOP_VERSION
ARG ZOOKEEPER_VERSION

ARG HADOOP_DOWNLOAD_URL="https://www.apache.org/dyn/closer.cgi?action=download&filename=hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"
ARG HADOOP_BACKUP_DOWNLOAD_URL="https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"

ARG ACCUMULO_DOWNLOAD_URL="https://www.apache.org/dyn/closer.cgi?action=download&filename=accumulo/${ACCUMULO_VERSION}/accumulo-${ACCUMULO_VERSION}-bin.tar.gz"
ARG ACCUMULO_BACKUP_DOWNLOAD_URL="https://archive.apache.org/dist/accumulo/${ACCUMULO_VERSION}/accumulo-${ACCUMULO_VERSION}-bin.tar.gz"

ARG ZOOKEEPER_DOWNLOAD_URL="https://www.apache.org/dyn/closer.cgi?action=download&filename=zookeeper/zookeeper-${ZOOKEEPER_VERSION}/apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz"
ARG ZOOKEEPER_BACKUP_DOWNLOAD_URL="https://archive.apache.org/dist/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz"

RUN	yum -y update && \
	yum -y install \
		gcc-c++ \
		make \
		java-1.8.0-openjdk-devel \
		wget \
	&& rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk/

# Allow users to provide their own builds of Accumulo, ZooKeeper and Hadoop
COPY ./files/ .
# Otherwise, download official distributions
RUN if [ ! -f "./accumulo-${ACCUMULO_VERSION}-bin.tar.gz" ]; then \
		(wget -nv -O ./accumulo-${ACCUMULO_VERSION}-bin.tar.gz ${ACCUMULO_DOWNLOAD_URL} || wget -nv -O ./accumulo-${ACCUMULO_VERSION}-bin.tar.gz ${ACCUMULO_BACKUP_DOWNLOAD_URL}); \
	fi && \
	if [ ! -f "./hadoop-${HADOOP_VERSION}.tar.gz" ]; then \
		(wget -nv -O ./hadoop-${HADOOP_VERSION}.tar.gz ${HADOOP_DOWNLOAD_URL} || wget -nv -O ./hadoop-${HADOOP_VERSION}.tar.gz ${HADOOP_BACKUP_DOWNLOAD_URL}); \
	fi && \
	if [ ! -f "./apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz" ]; then \
		(wget -nv -O ./apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz ${ZOOKEEPER_DOWNLOAD_URL} || wget -nv -O ./apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz ${ZOOKEEPER_BACKUP_DOWNLOAD_URL}); \
	fi

# Extract required files
RUN tar -xf ./accumulo-${ACCUMULO_VERSION}-bin.tar.gz accumulo-${ACCUMULO_VERSION}/bin/ accumulo-${ACCUMULO_VERSION}/lib/ && \
	rm -f ./accumulo-${ACCUMULO_VERSION}-bin.tar.gz && \
	accumulo-${ACCUMULO_VERSION}/bin/build_native_library.sh && \
	tar -xf ./hadoop-${HADOOP_VERSION}.tar.gz && \
	rm -f ./hadoop-${HADOOP_VERSION}.tar.gz && \
	cp ./hadoop-${HADOOP_VERSION}/share/hadoop/yarn/timelineservice/lib/commons-lang-*.jar ./hadoop-${HADOOP_VERSION}/share/hadoop/common/lib/ && \
	tar -xf ./apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz --wildcards "apache-zookeeper-${ZOOKEEPER_VERSION}-bin/lib/zookeeper*.jar" && \
	rm -f ./apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz
	
FROM ${BASE_IMAGE_NAME}:${BASE_IMAGE_TAG}
ARG ACCUMULO_VERSION
ARG HADOOP_VERSION
ARG ZOOKEEPER_VERSION
ARG USER=accumulo
ARG GROUP=accumulo

RUN yum -y update && \
	yum -y install \
		java-1.8.0-openjdk-devel \
		xmlstarlet \
		wget \
	&& rm -rf /var/lib/apt/lists/*
RUN wget -O /usr/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.5/dumb-init_1.2.5_x86_64
RUN chmod +x /usr/bin/dumb-init
COPY --from=builder --chown=root:root /accumulo-${ACCUMULO_VERSION} /opt/accumulo-${ACCUMULO_VERSION}/
COPY ./conf /opt/accumulo-${ACCUMULO_VERSION}/conf
COPY --from=builder --chown=root:root /hadoop-${HADOOP_VERSION} /opt/hadoop-${HADOOP_VERSION}/
COPY --from=builder --chown=root:root /apache-zookeeper-${ZOOKEEPER_VERSION}-bin /opt/zookeeper-${ZOOKEEPER_VERSION}/

RUN groupadd ${GROUP} && useradd --home-dir /opt/accumulo --gid ${GROUP} --no-create-home --shell /bin/bash ${USER}

RUN cd /opt \
	&& ln -s ./accumulo-${ACCUMULO_VERSION} ./accumulo \
	&& ln -s ./hadoop-${HADOOP_VERSION} ./hadoop \
	&& ln -s ./zookeeper-${ZOOKEEPER_VERSION} ./zookeeper \
	&& mkdir -p -m 755 /var/log/accumulo \
	&& chown ${USER}:${GROUP} /var/log/accumulo

USER ${USER}
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk/
ENV ACCUMULO_HOME /opt/accumulo
ENV ACCUMULO_CONF_DIR ${ACCUMULO_HOME}/conf
ENV ACCUMULO_LOG_DIR /var/log/accumulo
ENV HADOOP_HOME /opt/hadoop
ENV PATH $ACCUMULO_HOME/bin:$PATH

COPY ./entrypoint.sh /
ENTRYPOINT ["/entrypoint.sh", "accumulo"]
CMD ["help"]
