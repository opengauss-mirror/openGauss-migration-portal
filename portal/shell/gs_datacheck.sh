#!/bin/bash
APP_NAME=portalControl-6.0.0.rc1-exec.jar

#根据参数决定校验模式
case "$2" in
"full")
TYPE=full_migration
;;
"incremental")
TYPE=incremental_migration
;;
*)
usage
;;
esac

START_ORDER=start_mysql_${TYPE}_datacheck
STOP_ORDER=stop_plan
INSTALL_ORDER=install_mysql_datacheck_tools
UNINSTALL_ORDER=uninstall_mysql_${TYPE}_tools
SIGN="workspace.id=1"
ID=1
PORTAL_PATH="$PWD/"
SKIP=true

if [ ! -z $3 ]
  then
           ID=$3
           SIGN="workspace.id=$3"
fi

#使用说明，用来提示输入参数
usage() {
echo "Usage: ./脚本名.sh [install|start|stop|uninstall] [full|incremental] workspace.id"
echo "workspace.id is id of migration plan"
exit 1
}

#检查程序是否在运行
is_exist() {
pid=`ps -ef|grep $SIGN |grep $APP_NAME |grep -v grep|awk '{print \$2}' `
#如果不存在返回1，存在返回0
if [ -z "${pid}" ]; then
return 1
else
return 0
fi
}

#安装方法
install(){
java -Dpath=${PORTAL_PATH} -Dskip=${SKIP} -Dworkspace.id=${ID} -Dorder=${INSTALL_ORDER} -jar $APP_NAME &
wait
}

#启动方法
start(){
is_exist
if [ $? -eq "0" ]; then
echo "Migration plan $3 is already running. pid=${pid} ."
else
java -Dpath=${PORTAL_PATH} -Dskip=${SKIP} -Dworkspace.id=${ID} -Dorder=${START_ORDER} -jar $APP_NAME &
wait
fi
}

#停止方法
stop(){
java -Dpath=${PORTAL_PATH} -Dskip=${SKIP} -Dworkspace.id=${ID} -Dorder=${STOP_ORDER} -jar $APP_NAME &
wait
}

#卸载方法
uninstall(){
java -Dpath=${PORTAL_PATH} -Dskip=${SKIP} -Dworkspace.id=${ID} -Dorder=${UNINSTALL_ORDER} -jar $APP_NAME &
wait
}


#根据输入参数，选择执行对应方法，不输入则执行使用说明
if [ -z "${TYPE}" ]; then
echo "invalid datacheck type"
usage
else
case "$1" in
"install")
install
;;
"start")
start
;;
"stop")
stop
;;
"uninstall")
uninstall
;;
*)
echo "invalid command"
usage
;;
esac
fi
