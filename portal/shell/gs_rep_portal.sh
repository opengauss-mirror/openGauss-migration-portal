#!/bin/bash
APP_NAME=portalControl-6.0.0.rc1-exec.jar
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
