#!/bin/bash
cd `dirname $0`
BIN_DIR=`pwd`
cd ..
DEPLOY_DIR=`pwd`
CONF_DIR=$DEPLOY_DIR/conf
CLASSES_DIR=$DEPLOY_DIR/classes
LIB_DIR=$DEPLOY_DIR/lib
LOGS_FILE=$DEPLOY_DIR/logs/stdout.log
MAIN_CLASS=com.cssweb.statement.Main

if [ -f $LOGS_FILE ]; then
	echo "BACKUP: stdout.log!"
	mv $LOGS_FILE $LOGS_FILE`date +%Y%m%d%H%M%S`
fi

PIDS=`ps -ef | grep java | grep "$CONF_DIR" |awk '{print $2}'`

if [ -n "$PIDS" ]; then
    echo "ERROR: The Server already started!"
    echo "PID: $PIDS"
    exit 1
fi

LOGS_DIR=`dirname $LOGS_FILE`

if [ ! -d $LOGS_DIR ]; then
    mkdir $LOGS_DIR
fi

LIB_JARS=`ls $LIB_DIR|grep .jar|awk '{print "'$LIB_DIR'/"$0}'|tr "\n" ":"`

JAVA_OPTS=" -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true "
JAVA_DEBUG_OPTS=""
if [ "$1" = "debug" ]; then
    JAVA_DEBUG_OPTS=" -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=8000,server=y,suspend=n "
fi
JAVA_JMX_OPTS=""
if [ "$1" = "jmx" ]; then
    JAVA_JMX_OPTS=" -Dcom.sun.management.jmxremote.port=1099 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false "
fi
JAVA_MEM_OPTS=""
BITS=`java -version 2>&1 | grep -i 64-bit`
if [ -n "$BITS" ]; then
    JAVA_MEM_OPTS=" -server -Xms512m -Xmx512m -Xmn128m -XX:PermSize=128m -Xss256k -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseCMSCompactAtFullCollection -XX:LargePageSizeInBytes=128m -XX:+UseFastAccessorMethods -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=70 "
else
    JAVA_MEM_OPTS=" -server -Xms256m -Xmx256m -XX:PermSize=64m -XX:SurvivorRatio=2 -XX:+UseParallelGC "
fi

echo -e "Starting the Server ...\c"
nohup java $JAVA_OPTS $JAVA_MEM_OPTS $JAVA_DEBUG_OPTS $JAVA_JMX_OPTS -classpath $CONF_DIR:$CLASSES_DIR:$LIB_JARS $MAIN_CLASS > $LOGS_FILE 2>&1 &

COUNT=0
while [ $COUNT -lt 1 ]; do    
    echo -e ".\c"
    sleep 1 
    COUNT=`ps -f | grep java | grep "$DEPLOY_DIR" | awk '{print $2}' | wc -l`
    if [ $COUNT -gt 0 ]; then
        break
    fi
done

echo "OK!"
PIDS=`ps -f | grep java | grep "$DEPLOY_DIR" | awk '{print $2}'`
echo "PID: $PIDS"
echo "STDOUT: $LOGS_FILE"
