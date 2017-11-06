FROM jboss/base-jdk:8

# Switch to the root user (base module leaves the user as datawave) to do some configuration
USER root

LABEL version=${project.version} \
      run="docker run ${docker.image.prefix}${project.artifactId}:latest" \
      description="DATAWAVE authorization microservice."

VOLUME /tmp
ADD ${project.build.finalName}-exec.jar /app.jar
RUN bash -c 'touch /app.jar && chown jboss:jboss /app.jar'

# Switch back to jboss user
USER jboss

EXPOSE 8443 8080
ENTRYPOINT ["java","-jar","/app.jar"]
