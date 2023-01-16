
export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server:$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH
export HADOOP_CLASSPATH=$ALLUXIO_HOME/client/alluxio-enterprise-2.9.0-1.0-client.jar:$HADOOP_CLASSPATH
export CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath --glob):$CLASSPATH

./test_hdfs_read $@
