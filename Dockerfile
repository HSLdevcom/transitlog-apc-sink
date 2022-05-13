FROM openjdk:11-jre-slim
#Install curl for health check
RUN apt-get update && apt-get install -y --no-install-recommends curl
ADD build/libs/transitlog-apc-sink.jar /usr/app/transitlog-apc-sink.jar
ENTRYPOINT ["java", "-XX:InitialRAMPercentage=50.0", "-XX:MaxRAMPercentage=95.0", "-jar", "/usr/app/transitlog-hfp-csv-sink.jar"]
