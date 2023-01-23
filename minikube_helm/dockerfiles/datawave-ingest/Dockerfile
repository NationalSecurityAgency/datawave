FROM containeryard.evoforge.org/warehouses/minikube/accumulo:1.10.2
USER root
COPY --from=containeryard.evoforge.org/warehouses/hadoop:3.3.3  /usr/local/hadoop/  /usr/local/hadoop/
COPY --from=containeryard.evoforge.org/warehouses/zookeeper:3.6.3  /usr/lib/zookeeper/  /usr/lib/zookeeper/
RUN adduser datawave
RUN yum -y install epel-release && \
 yum install -y pdsh && \
 yum install -y genders && \
 yum install -y gettext
RUN mkdir -p /var/run/datawave && mkdir -p /srv/logs && chown datawave:datawave /var/run/datawave && chown datawave:datawave /srv/logs
ENTRYPOINT ["tail", "-f", "/dev/null"]