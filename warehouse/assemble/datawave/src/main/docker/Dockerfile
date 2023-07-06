FROM alerman/accumulo:1.10.2
USER root
COPY --from=alerman/hadoop:3.3.4.v1  /usr/local/hadoop/  /usr/local/hadoop/
COPY --from=bitnami/zookeeper:3.6.3  /opt/bitnami/zookeeper/  /usr/lib/zookeeper/
RUN cp /usr/lib/zookeeper/lib/*.jar /usr/lib/zookeeper
RUN adduser datawave
RUN yum -y install epel-release && \
 yum install -y pdsh && \
 yum install -y genders && \
 yum install -y gettext
RUN mkdir -p /var/run/datawave && mkdir -p /srv/logs && chown datawave:datawave /var/run/datawave && chown datawave:datawave /srv/logs && mkdir /tmp/rpms
COPY ./datawave-dw-${build.env}-${project.version}*.rpm /tmp/ingest.rpm
RUN yum localinstall -y /tmp/ingest.rpm

ENTRYPOINT ["tail", "-f", "/dev/null"]
