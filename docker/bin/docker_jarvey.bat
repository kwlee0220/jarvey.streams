@echo off

docker run -it --rm --net host -v %JARVEY_MOUNT%:/jarvey kwlee0220/jarvey.streams ^
java -cp /jarvey.streams.jar jarvey.streams.assoc.tool.JarveyStreamCommandsMain %*