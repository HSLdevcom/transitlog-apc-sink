# syntax=docker/dockerfile:1
# check=error=true

FROM hsldevcom/infodevops-docker-base-images:1.0.2-25-java-jdk AS build

WORKDIR /usr/app
COPY gradlew gradle.properties settings.gradle build.gradle ./
COPY gradle ./gradle
COPY src/main ./src/main
RUN --mount=type=secret,id=github_token \
    --mount=type=secret,id=github_actor \
    export GITHUB_TOKEN="$(cat /run/secrets/github_token)" && \
    export GITHUB_ACTOR="$(cat /run/secrets/github_actor)" && \
    ./gradlew shadowJar --no-daemon

FROM hsldevcom/infodevops-docker-base-images:1.0.2-25-java-jre
WORKDIR /usr/app
COPY --from=build /usr/app/build/libs/transitlog-apc-sink.jar /usr/app/transitlog-apc-sink.jar
COPY --chmod=755 start-application.sh /start-application.sh
ENTRYPOINT ["/start-application.sh"]
