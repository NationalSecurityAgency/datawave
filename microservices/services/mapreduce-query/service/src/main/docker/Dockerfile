FROM azul/zulu-openjdk-alpine:11

LABEL version=${project.version} \
      run="docker run ${registry.namespace}${image.prefix}${project.artifactId}:${project.version}" \
      pull="docker pull ${registry.namespace}${image.prefix}${project.artifactId}:${project.version}" \
      description="${project.description}"

ADD ${project.build.finalName}-exec.jar /app.jar
ADD mapreduce-query-core-job-*.jar /MapReduceQueryCoreJob.jar
RUN chmod 755 /app.jar
RUN chmod 755 /MapReduceQueryCoreJob.jar
RUN apk add libc6-compat curl

EXPOSE 8443 8080
ENTRYPOINT ["java","-jar","app.jar"]
