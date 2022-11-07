FROM sgrio/java:jre_8

RUN mkdir -p /jarvey
WORKDIR /jarvey
COPY ../jarvey.streams-0.1.0-all.jar ./jarvey.streams.jar

CMD ["java", "-cp", "/jarvey/jarvey.streams.jar", "jarvey.streams.zone.NodeZoneTrackerDockerMain"]
