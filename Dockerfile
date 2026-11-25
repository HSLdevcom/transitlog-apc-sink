# syntax=docker/dockerfile:1
# check=error=true

FROM hsldevcom/infodevops-docker-base-images:1.1.1-25-java-jdk AS build

WORKDIR /usr/app
COPY gradlew gradle.properties settings.gradle build.gradle ./
COPY gradle ./gradle
COPY src/main ./src/main
RUN --mount=type=secret,id=github_token \
    --mount=type=secret,id=github_actor \
    GITHUB_TOKEN="$(cat /run/secrets/github_token)" && \
    GITHUB_ACTOR="$(cat /run/secrets/github_actor)" && \
    export GITHUB_TOKEN GITHUB_ACTOR && \
    ./gradlew shadowJar --no-daemon

FROM hsldevcom/infodevops-docker-base-images:1.1.1-25-java-jre
RUN useradd --system --no-create-home --uid 10001 app
WORKDIR /usr/app
COPY --from=build /usr/app/build/libs/transitlog-apc-sink.jar /usr/app/transitlog-apc-sink.jar
COPY --chmod=755 start-application.sh /start-application.sh
USER 10001
ENTRYPOINT ["/start-application.sh"]
