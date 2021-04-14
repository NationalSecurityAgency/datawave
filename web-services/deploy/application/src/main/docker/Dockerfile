FROM azul/zulu-openjdk-centos:8u252

RUN yum -y install deltarpm centos-release-sc && \
    chmod -R 777 /usr/lib/jvm/zulu-8/bin/* && \
    groupadd -r jboss -g 1000 && \
    useradd -u 1000 -r -g jboss -m -d /opt/jboss -s /sbin/nologin -c "JBoss User" jboss && \
    groupadd -r hadoop && \
    useradd -r -g hadoop -m -d /opt/datawave -s /bin/bash -c "DATAWAVE User" datawave && \
    chown -R datawave:hadoop /opt/datawave /opt/jboss && \
    chmod -R ug+rX,o-rx /opt/datawave /opt/jboss

ENV JAVA_VERSION=8 \
    JAVA_UPDATE=252 \
    JAVA_BUILD=1.8.0_252-b14 \
    JAVA_HOME=/usr/lib/jvm/zulu8
LABEL version="1.8.0_252"

RUN yum update -y && \
    yum install -y which less bind-utils net-tools lsof nethogs dstat strace htop iperf iperf3 socat iftop xmlstarlet saxon augeas bsdtar unzip && \
    yum -y erase deltarpm 

ENV WILDFLY_VERSION 17.0.1.Final \
    JBOSS_HOME /opt/jboss/wildfly \
    LAUNCH_JOBS_IN_BACKGROUND true

COPY --from=jboss/wildfly:17.0.1.Final /opt/jboss/wildfly /opt/jboss/wildfly
EXPOSE 8080

CMD ["/opt/jboss/wildfly/bin/standalone.sh","-b","0.0.0.0"]

RUN chown -R datawave:hadoop /opt/jboss && \
    chmod -R ug+rX,o-rx /opt/jboss

LABEL name="Datawave web service image"

ENV WILDFLY_HOME=/opt/jboss/wildfly \
    PRESERVE_DATA_DIR=true \
    PRESERVE_LOG_DIR=true

WORKDIR $WILDFLY_HOME


COPY overlay $WILDFLY_HOME/
COPY mysql $WILDFLY_HOME/mysql
COPY *.cli $WILDFLY_HOME/tools/
COPY ${project.build.finalName}-${build.env}.ear $WILDFLY_HOME/standalone/deployments
COPY docker-entrypoint.sh /

RUN cp /opt/jboss/.bash* $WILDFLY_HOME && \
    chown -R datawave:hadoop /docker-entrypoint.sh /opt/jboss && \
    chmod -R u+rwX /opt/jboss && \
    chmod u+x /docker-entrypoint.sh  $WILDFLY_HOME/bin/*  $WILDFLY_HOME/tools/*.sh  $WILDFLY_HOME/tools/*.py

USER datawave

RUN touch $WILDFLY_HOME/standalone/deployments/${project.build.finalName}-${build.env}.ear.skipdeploy && \
    $WILDFLY_HOME/bin/jboss-cli.sh --file=$WILDFLY_HOME/tools/add-datawave-configuration.cli && \
    rm -rf $WILDFLY_HOME/standalone/configuration/standalone_xml_history && \
    rm -rf $WILDFLY_HOME/standalone/log && \
    rm -f $WILDFLY_HOME/tools/*.cli && \
    rm $WILDFLY_HOME/standalone/deployments/${project.build.finalName}-${build.env}.ear.skipdeploy

EXPOSE 8080 8443 9990

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["/opt/jboss/wildfly/bin/standalone.sh", "-b", "0.0.0.0", "-c", "standalone-full.xml"]
