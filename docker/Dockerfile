FROM sgrio/java:jdk_8_ubuntu

COPY jarvey.streams-2023.12.29-all.jar /jarvey.streams.jar

RUN mkdir -p /jarvey

WORKDIR /jarvey
VOLUME /jarvey

#ENTRYPOINT ["java", "-cp", "/jarvey.streams.jar"]
