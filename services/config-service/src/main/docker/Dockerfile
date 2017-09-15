FROM jboss/base-jdk:8

# Switch to the root user (base module leaves the user as datawave) to do some configuration
USER root

LABEL VERSION=${project.version} \
      RUN="docker run ${docker.image.prefix}/${project.artifactId}:latest" \
      DESCRIPTION="DATAWAVE configuration microservice."

VOLUME /tmp
ADD config-service-${project.version}-exec.jar /app.jar
RUN bash -c 'touch /app.jar && chown jboss:jboss /app.jar'

# Switch back to jboss user
USER jboss

EXPOSE 8888
ENTRYPOINT ["java","-jar","/app.jar"]
