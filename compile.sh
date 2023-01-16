#!/bin/sh
if [ $# -ne 1 ]; then
	echo "Usage: $0 SOURCE_FILE" >&2
	exit 1
fi
gcc $1 \
	-I$HADOOP_HOME/include \
	-I$JAVA_HOME/include \
	-I$JAVA_HOME/include/linux \
       	-L$HADOOP_HOME/lib/native \
       	-L$JAVA_HOME/jre/lib/amd64/server \
	-lhdfs -ljvm \
	-o $(basename $1 .c)
