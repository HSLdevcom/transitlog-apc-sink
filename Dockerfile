# syntax=docker/dockerfile:1
# check=error=true

FROM hsldevcom/infodevops-docker-base-images:1.0.2-25-java-jdk AS test

FROM hsldevcom/infodevops-docker-base-images:1.0.2-25-java-jre
WORKDIR /usr/app
COPY build/libs/transitlog-apc-sink.jar /usr/app/transitlog-apc-sink.jar
COPY --chmod=755 start-application.sh /start-application.sh
ENTRYPOINT ["/start-application.sh"]
