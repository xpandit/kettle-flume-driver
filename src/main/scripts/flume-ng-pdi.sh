PDI_INSTALL_DIR=/home/puls3/dev/data-integration-442-ga

BASEDIR=`dirname $0`
FLUME_AGENT_CONF=${BASEDIR}/conf/flume.conf
FLUME_CONF_DIR=${BASEDIR}/conf
FLUME_EXTRA_CLASSPATH=${BASEDIR}/kettle-flume-driver-1.0-SNAPSHOT.jar

# Add PDI extra JARs to classpath
for f in `find $PDI_INSTALL_DIR/lib -type f -name "*.jar"` `find $PDI_INSTALL_DIR/lib -type f -name "*.zip"`
do
  FLUME_EXTRA_CLASSPATH=$FLUME_EXTRA_CLASSPATH:$f
done

#echo $FLUME_EXTRA_CLASSPATH

# Start flume agent
flume-ng agent -n agent -c $FLUME_CONF_DIR -f $FLUME_AGENT_CONF -C $FLUME_EXTRA_CLASSPATH
