FROM eclipse-temurin:11-alpine
#Install curl for health check
RUN apk add --no-cache curl

ADD build/libs/transitlog-apc-sink.jar /usr/app/transitlog-apc-sink.jar
ADD start-application.sh /
RUN chmod +x /start-application.sh
ENTRYPOINT ["/start-application.sh"]
