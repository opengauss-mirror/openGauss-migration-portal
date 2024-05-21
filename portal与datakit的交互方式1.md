假设portal的jar包所在目录为portal_home

1. 安装部分：

   Datakit在安装完portal之后，先配置portal_home/config/toolspath.properties中各迁移工具的位置，通过调用java -Dorder=install_mysql_all_migration_tools -Dskip=true -jar portalControl-6.0.0-exec.jar安装全部迁移工具。

   toolspath.properties需要配置的路径：

   | 参数名称                     | 参数说明                                                     |
   | ---------------------------- | ------------------------------------------------------------ |
   | chameleon.venv.path          | 变色龙虚拟环境所在位置                                       |
   | chameleon.pkg.path           | 变色龙的安装包所在路径                                       |
   | chameleon.pkg.name           | 变色龙的安装包名                                             |
   | debezium.path                | debezium+kafka所在路径（默认kafka、confluent、connector都安装在该路径下） |
   | kafka.path                   | kafka所在路径                                                |
   | confluent.path               | confluent所在路径                                            |
   | connector.path               | connector(包括mysql和openGauss)所在路径                      |
   | debezium.pkg.path            | debezium+kafka安装包所在路径（默认kafka、confluent、connector安装包都在该路径下） |
   | kafka.pkg.name               | kafka安装包名                                                |
   | confluent.pkg.name           | confluent安装包名                                            |
   | connector.mysql.pkg.name     | mysql connector安装包名                                      |
   | connector.opengauss.pkg.name | opengauss connector安装包名                                  |
   | datacheck.install.path       | datacheck安装路径                                            |
   | datacheck.path               | datacheck所在路径                                            |
   | datacheck.pkg.path           | datacheck安装包所在路径                                      |
   | datacheck.pkg.name           | datacheck安装包名                                            |

2. 运行部分：

   datakit通过-Dparameter=value的形式将参数传给portal（这里需要全部参数文档）

   比如java -Dparameter=value -jar  portalControl-6.0.0-exec.jar

   迁移进度获取：

   约定工作空间后，portal在指定位置写自己的状态文件（json格式），然后datakit在获取到portal的状态文件过后，根据portal自身所处的迁移状态去决定向用户呈现哪个迁移过程的状态。

   | 文件位置                                                  | 说明             |
   | --------------------------------------------------------- | ---------------- |
   | portal_home/workspace/id/status/portal.txt                | portal的迁移进度 |
   | portal_home/workspace/id/status/full_migration.txt        | 全量迁移的进度   |
   | portal_home/workspace/id/status/incremental_migration.txt | 增量迁移的进度   |
   | portal_home/workspace/id/status/reverse_migration.txt     | 反向迁移的进度   |

3. 交互方式：

   java -Dworkspace.id=1 -Dorder=start_plan3 -Dskip=true -jar portalControl-6.0.0-exec.jar

   | 指令        | 说明                                                         |
   | ----------- | ------------------------------------------------------------ |
   | start_plan1 | 启动默认计划1，包括全量迁移和全量校验，也就是离线模式        |
   | start_plan3 | 启动默认计划3，包括全量迁移和全量校验，增量迁移和增量校验，反向迁移和反向校验，也就是在线模式。 |

   如果执行在线迁移，那么直接启动plan3即可，如果想要追加操作，比如停止增量迁移，就需要另起一个进程向之前的进程传递信号。

   java -Dworkspace.id=1 -Dorder=stop_incremental_migration -Dskip=true -jar portalControl-6.0.0-exec.jar

   以上命令代表停止workspaceid为1的任务的增量迁移指令。

   datakit要另起进程与portal进行交互，根据workspaceid去确定要交互的任务。

   -Dorder=stop_incremental_migration 停止增量迁移（可以选择启动增量迁移或启动反向迁移）

   -Dorder=run_incremental_migration 启动增量迁移（已经在运行的增量迁移被用户手动停止后后才能输入这个命令启动）

   -Dorder=stop_reverse_migration 停止反向迁移（可以选择启动反向迁移）

   -Dorder=run_reverse_migration 启动反向迁移（只有在增量迁移停止后，才能启动反向迁移，反向迁移中途停止后也可以通过这种方式重新启动反向迁移）

   -Dorder=stop_plan 停止整个计划