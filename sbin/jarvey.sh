#! /bin/bash

java -Dlog4j.configurationFile=%JARVEY_STREAMS_HOME%/log4j2.xml \
-cp %JARVEY_STREAMS_HOME%/sbin/jarvey.streams.jar \
jarvey.streams.assoc.tool.JarveyStreamCommandsMain "$@"
