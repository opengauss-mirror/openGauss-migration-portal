#!/bin/bash
APP_NAME=portalControl-7.0.0rc3-exec.jar
ORDER=$1
SIGN="workspace.id=1"
ID=1
PORTAL_PATH="$PWD/"
SKIP=true

if [ ! -z $2 ]
  then
           ID=$2
           SIGN="workspace.id=$2"
fi

#使用说明，用来提示输入参数
usage() {
echo "Usage: sh 脚本名.sh order workspace.id"
echo "order is in order list."
echo "workspace.id is id of migration plan"
exit 1
}

change_properties_file() {
    properties_path="${PORTAL_PATH}config/toolspath.properties"
    has_changed=false

    check_result=$(grep -c "chameleon.install.path=/ops/portal/tools/chameleon/" ${properties_path})
    if [ $check_result -eq 0 ]; then
        has_changed=true
    fi

    if [ $has_changed == false ]; then
        echo "Modifying the properties file."
        sed -i "s#/ops/portal/#${PORTAL_PATH}#g" ${properties_path}
    fi
}

find_available_port() {
    local start_port=$1
    local port=$start_port
    local max_port=65535

    while [ "$port" -le "$max_port" ]; do
        if ! timeout 1 bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/$port" 2>/dev/null; then
            echo "$port"
            return 0
        fi
        port=$((port + 1))
    done

    echo "Error: No available port found within the range from $start_port to $max_port"
    return 1
}

install_portal() {
    change_properties_file

    ip="127.0.0.1"
    zookeeper_port=2181
    kafka_port=9092
    schema_registry_port=8081
    
    if ! zookeeper_port=$(find_available_port $zookeeper_port); then
        echo "Error: Failed to find available port for Zookeeper."
        exit 1
    fi

    if ! kafka_port=$(find_available_port $kafka_port); then
        echo "Error: Failed to find available port for Kafka."
        exit 1
    fi

    if ! schema_registry_port=$(find_available_port $schema_registry_port); then
        echo "Error: Failed to find available port for Schema Registry."
        exit 1
    fi

    echo "Start the installation."
    java -Dpath=${PORTAL_PATH}  -Dskip=${SKIP} \
    -DzookeeperPort=${zookeeper_port} -DkafkaPort=${kafka_port} -DschemaRegistryPort=${schema_registry_port} \
    -DzkIp=${ip} -DkafkaIp=${ip} -DschemaRegistryIp=${ip} -DthirdPartySoftwareConfigType=2 \
    -DinstallDir=${PORTAL_PATH}tools/debezium/ -Dorder=install_mysql_all_migration_tools -jar ${APP_NAME}
}

#检查程序是否在运行
is_exist() {
pid=`ps -ef|grep $SIGN |grep $ORDER |grep $APP_NAME |grep -v grep|awk '{print \$2}' `
#如果不存在返回1，存在返回0
if [ -z "${pid}" ]; then
return 1
else
return 0
fi
}

#启动方法
start(){
is_exist
if [ $? -eq "0" ]; then
echo "Migration plan $ID is already running. pid=${pid} ."
elif [ $ORDER == "install_mysql_all_migration_tools" ]; then
install_portal
else
java -Dpath=${PORTAL_PATH} -Dskip=${SKIP} -Dworkspace.id=${ID} -Dorder=${ORDER} -jar $APP_NAME &
wait
fi
}

case "$1" in
"help")
start
wait
usage
;;
*)
start
;;
esac
