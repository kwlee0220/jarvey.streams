#! /bin/bash

java -Dlog4j.configurationFile=D:\Dropbox\development\jarvey\jarvey.streams\log4j2.xml \
-cp %JARVEY_STREAMS_HOME%/sbin/jarvey.streams-2023.12.29-all.jar \
jarvey.streams.assoc.tool.JarveyStreamCommandsMain "$@"
