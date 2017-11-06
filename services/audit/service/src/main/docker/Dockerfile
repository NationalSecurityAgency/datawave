FROM openjdk:8-jdk-alpine

LABEL version=${project.version} \
      run="docker run ${docker.image.prefix}${project.artifactId}:latest" \
      description="DATAWAVE audit microservice."

ADD ${project.build.finalName}-exec.jar /app.jar

EXPOSE 8080
ENTRYPOINT ["java","-jar","app.jar"]