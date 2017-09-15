FROM openjdk:8-jdk-alpine

LABEL VERSION=${project.version} \
      RUN="docker run ${docker.image.prefix}/${project.artifactId}:latest" \
      DESCRIPTION="DATAWAVE audit microservice."

ADD audit-service-${project.version}-exec.jar /app.jar

EXPOSE 8080
ENTRYPOINT ["java","-jar","app.jar"]