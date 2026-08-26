# https://github.com/NationalSecurityAgency/datawave-stack-docker-images
# This image already contains the Hadoop client/server binaries and ZooKeeper
# client libraries required by the ingest distribution.
ARG DATAWAVE_STACK_ACCUMULO_VERSION=2.1.6
FROM ghcr.io/nationalsecurityagency/datawave-stack-accumulo:${DATAWAVE_STACK_ACCUMULO_VERSION}

USER root

RUN adduser datawave -g hadoop \
    --home /opt/datawave-ingest && echo -n "cd" >> /opt/datawave-ingest/.bashrc

RUN mkdir -p /var/run/datawave && mkdir -p /srv/logs && mkdir -p /srv/data/datawave/flags && chown datawave:hadoop -R /srv/data && chown datawave:hadoop /var/run/datawave && chown datawave:hadoop /srv/logs && mkdir /tmp/rpms
COPY datawave-dw-${build.env}-*.rpm /tmp/ingest.rpm
RUN yum localinstall -y /tmp/ingest.rpm && \
    chown -R datawave:hadoop /opt/datawave-ingest && \
    chmod -R a+rX /opt/accumulo/conf
USER datawave
ENTRYPOINT ["tail", "-f", "/dev/null"]
