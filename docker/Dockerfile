FROM sgrio/java:jdk_8_ubuntu

COPY jarvey.streams-0.1.0-all.jar /jarvey.streams.jar

RUN mkdir -p /jarvey

WORKDIR /jarvey
VOLUME /jarvey

#ENTRYPOINT ["java", "-cp", "/jarvey.streams.jar"]
