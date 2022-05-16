FROM openjdk:11-jre-slim
#Install curl for health check
RUN apt-get update && apt-get install -y --no-install-recommends curl
ADD build/libs/transitlog-apc-sink.jar /usr/app/transitlog-apc-sink.jar
ADD start-application.sh /
RUN chmod +x /start-application.sh
ENTRYPOINT ["/start-application.sh"]
