FROM sgrio/java:jre_8

RUN mkdir -p /jarvey
WORKDIR /jarvey
COPY jarvey.streams-0.1.0-all.jar .

CMD ["java", "-cp", "/jarvey/jarvey.streams-0.1.0-all.jar", "jarvey.streams.zone.NodeZoneTrackerDockerMain"]