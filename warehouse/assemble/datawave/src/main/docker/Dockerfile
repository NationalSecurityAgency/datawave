FROM ${docker.image.prefix}datawave-stack-hadoop:3.3.6 AS hadoop
FROM bitnami/zookeeper:3.6.3 AS zookeeper
FROM ${docker.image.prefix}datawave-stack-accumulo:${docker.image.accumulo.tag}

USER root

# Keep one non-COPY operation between COPY operations. A failure may occur if the destination dir is unchanged and
# is followed by a second COPY operation. This is related to OverlayFS and its native overlay diff feature.
COPY --from=hadoop  /usr/local/hadoop/  /usr/local/hadoop/
RUN adduser datawave -g hadoop \
    --home /opt/datawave-ingest && echo -n "cd" >> /opt/datawave-ingest/.bashrc
COPY --from=zookeeper  /opt/bitnami/zookeeper/  /usr/lib/zookeeper/

RUN yum -y install epel-release && \
 yum install -y pdsh && \
 yum install -y genders && \
 yum install -y gettext
RUN mkdir -p /var/run/datawave && mkdir -p /srv/logs && mkdir -p /srv/data/datawave/flags && chown datawave:hadoop -R /srv/data && chown datawave:hadoop /var/run/datawave && chown datawave:hadoop /srv/logs && mkdir /tmp/rpms
COPY datawave-dw-${build.env}-*.rpm /tmp/ingest.rpm
RUN yum localinstall -y /tmp/ingest.rpm
USER datawave
ENTRYPOINT ["tail", "-f", "/dev/null"]
