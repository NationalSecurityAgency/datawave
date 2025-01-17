FROM ${docker.image.prefix}datawave-stack-accumulo:${docker.image.accumulo.tag}
USER root
COPY --from=${docker.image.prefix}datawave-stack-hadoop:3.3.6  /usr/local/hadoop/  /usr/local/hadoop/

RUN adduser datawave --home /opt/datawave-ingest && echo -n "cd" >> /opt/datawave-ingest/.bashrc
COPY --from=bitnami/zookeeper:3.6.3  /opt/bitnami/zookeeper/  /usr/lib/zookeeper/
RUN yum -y install epel-release && \
 yum install -y pdsh && \
 yum install -y genders && \
 yum install -y gettext
RUN mkdir -p /var/run/datawave && mkdir -p /srv/logs && mkdir -p /srv/data/datawave/flags && chown datawave:datawave -R /srv/data && chown datawave:datawave /var/run/datawave && chown datawave:datawave /srv/logs && mkdir /tmp/rpms
COPY ./datawave-dw-${build.env}-${project.version}*.rpm /tmp/ingest.rpm
RUN yum localinstall -y /tmp/ingest.rpm
USER datawave
ENTRYPOINT ["tail", "-f", "/dev/null"]
