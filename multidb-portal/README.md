# 1 简介

## 1.1 工具介绍

mutidb_portal 是一款基于Java开发的openGauss数据迁移门户工具，整合了openGauss全量迁移、增量迁移、反向迁移及数据校验功能，支持完成MySQL/PostgreSQL/Elasticsearch/Milvus到openGauss的一站式迁移。

## 1.2 迁移能力

（1）MySQL迁移能力

支持MySQL到openGauss的全量迁移、增量迁移、反向迁移、全量数据校验。

（2）PostgreSQL迁移能力

支持PostgreSQL到openGauss的全量迁移、增量迁移、反向迁移。

（3）Elasticsearch迁移能力

支持Elasticsearch到openGauss的全量迁移。

（4）Milvus迁移能力

支持Milvus到openGauss的全量迁移。

## 1.3 使用限制

（1）服务器限制

工具当前仅支持在指定系统架构的Linux服务器中运行，支持的系统架构如下：

- CentOS7 x86_64
- openEuler20.03 x86_64/aarch64
- openEuler22.03 x86_64/aarch64
- openEuler24.03 x86_64/aarch64

（2）运行环境限制

工具使用Java 17编写，需要服务器准备Java 17或更高版本的运行环境。

（3）数据库版本限制

- MySQL 5.7及以上版本。
- PostgreSQL 9.4.26及以上版本。
- Elasticsearch 7.3及以上版本。
- Milvus 2.3及以上版本。
- openGauss适配MySQL需要5.0.0及以上版本。
- openGauss适配PostgreSQL需要6.0.0-RC1及以上版本。
- openGauss适配Elasticsearch/Milvus需要7.0.0-RC1及以上版本。

# 2 工具安装

## 2.1 安装包获取

各系统架构对应的安装包下载链接如下表：

| 系统名称       | 架构    | 下载链接                                                     |
| :------------- | ------- | ------------------------------------------------------------ |
| CentOS7        | x86_64  | https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/centos7/openGauss-portal-7.0.0rc3-CentOS7-x86_64.tar.gz |
| openEuler20.03 | x86_64  | https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/openEuler20.03/openGauss-portal-7.0.0rc3-openEuler20.03-x86_64.tar.gz |
| openEuler20.03 | aarch64 | https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/openEuler20.03/openGauss-portal-7.0.0rc3-openEuler20.03-aarch64.tar.gz |
| openEuler22.03 | x86_64  | https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/openEuler22.03/openGauss-portal-7.0.0rc3-openEuler22.03-x86_64.tar.gz |
| openEuler22.03 | aarch64 | https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/openEuler22.03/openGauss-portal-7.0.0rc3-openEuler22.03-aarch64.tar.gz |
| openEuler24.03 | x86_64  | https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/openEuler24.03/openGauss-portal-7.0.0rc3-openEuler24.03-x86_64.tar.gz |
| openEuler24.03 | aarch64 | https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/openEuler24.03/openGauss-portal-7.0.0rc3-openEuler24.03-aarch64.tar.gz |

## 2.2 安装步骤

此处以在CentOS7 x86_64的服务器上安装为例，讲解安装步骤。

（1）下载安装包

下载匹配自身系统架构的安装包，参考命令如下

```sh
wget https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/centos7/openGauss-portal-7.0.0rc3-CentOS7-x86_64.tar.gz
```

（2）解压安装包

完成安装包下载后，参考如下命令解压安装包

```sh
tar -zxvf openGauss-portal-7.0.0rc3-CentOS7-x86_64.tar.gz
```

（3）查看目录结构

切换至解压出的portal目录下，查看其目录结构，参考命令如下：

```sh
cd portal && ls -l
```

检查是否包含如下目录结构

```sh
bin                                 # 工具操作命令储存目录，其中包含的命令可逐个执行，以学习各命令提示的用法
config                              # 工具配置文件目录
openGauss-portal-7.0.0rc3.jar       # 工具核心jar文件
pkg                                 # 迁移组件储存目录
template                            # 迁移模版文件储存目录
```

**注意：上述罗列的目录结构中的内容，请勿修改，删减等，否则可能导致工具无法正常运行。**

（4）安装chameleon依赖

chameleon为MySQL全量迁移工具，不需要迁移MySQL时，可以跳过此项。

依赖安装，要求使用root用户，或者sudo免密用户。切换到portal目录下后，执行如下命令

```sh
./bin/install dependencies
```

（5）安装Elasticsearch/Milvus迁移依赖

请参考如下文档安装Elasticsearch/Milvus迁移依赖，不需要迁移Elasticsearch/Milvus时，可以跳过此项。

- [从Elasticsearch迁移至openGauss](https://docs.opengauss.org/zh/docs/latest/datavec/elasticsearch_to_opengauss.html)
- [从Milvus迁移至openGauss DataVec](https://docs.opengauss.org/zh/docs/latest/datavec/milvus2datavec.html)

（6）安装迁移工具

迁移工具安装命令如下：

```sh
./bin/install tools                           # 一键安装所有迁移工具命令，需提前完成chameleon依赖安装
./bin/install chameleon                       # MySQL全量迁移工具安装命令，需提前完成chameleon依赖安装
./bin/install full_replicate                  # PostgreSQL全量迁移工具安装命令
./bin/install elasticsearch_migration_tool    # Elasticsearch全量迁移工具安装命令
./bin/install milvus_migration_tool           # Milvus全量迁移工具安装命令
./bin/install debezium                        # 增量、反向迁移工具安装命令
./bin/install data_checker                    # 数据校验工具安装命令
./bin/install kafka                           # 工具所需三方工具安装命令，涉及增量迁移、反向迁移、数据校验时，需要安装
```

（7）检查安装状态

迁移工具安装完成后，使用如下命令检查各工具安装状态，确保所需迁移工具均已完成安装

```sh
./bin/install check
```

# 3 使用迁移功能

## 1.1 创建迁移任务

（1）创建迁移任务

创建迁移任务的命令模版如下，使用时请根据自身情况替换对应参数。

```sh
./bin/task create <task_id> <source_db_type>
```

其中，

- task_id：任务唯一标识符，不可重复，可以由字母数字下换线和连字符组成，长度不可超过50个字符。
- source_db_type：源端数据库类型，当前支持MySQL/PostgreSQL/Elasticsearch/Milvus，创建时可取值：mysql、MySQL、postgresql、PostgreSQL、elasticsearch、Elasticsearch、milvus、Milvus。

命令使用示例如下

```sh
./bin/task create 1 mysql
```

（2）查询已有任务

成功创建任务后，可参考如下命令查询已存在哪些任务

```sh
./bin/task list
```

**注**：其他task命令，请自行运行task脚本学习。

## 1.2 配置迁移任务

（1）迁移任务配置简介

此处以MySQL迁移配置为例，简要介绍配置文件的主要内容。配置文件中，各项配置也包含注释可自行学习。

**注意：此处介绍的配置，配置迁移任务时，为必配项。**

```properties
# 迁移模式，用于控制迁移任务包含全量迁移、增量迁移、反向迁移、全量校验中的哪些阶段，可通过./bin/mode命令管理
migration.mode=plan1

# MySQL服务配置如下
# MySQL服务所在主机IP
mysql.database.ip=127.0.0.1

# MySQL服务端口
mysql.database.port=3306

# 要迁移的MySQL数据库名称
mysql.database.name=test_db

# MySQL服务连接用户
mysql.database.username=test_user

# MySQL服务连接用户密码
mysql.database.password=******

# openGauss服务配置如下
# openGauss服务所在主机IP
opengauss.database.ip=127.0.0.1

# openGauss服务端口
opengauss.database.port=5432

# 迁移到openGauss的数据库名称，需要在openGauss侧提前创建好，且要求兼容性为b
# 创建语句参考：create database test_db with dbcompatibility = 'b';
opengauss.database.name=test_db

# openGauss服务连接用户
opengauss.database.username=test_user

# openGauss服务连接用户密码
opengauss.database.password=******

# 是否使用交互式密码输入。默认值为false，如果此参数设置为true，您无需在配置文件中明文配置数据库密码，运行迁移时，需要根据提示输入密码。
#use.interactive.password=false
```

（2）配置迁移任务

创建迁移任务成功后，会在portal的workspace目录下生成对应task_id的任务目录结构。

如上述创建的示例任务，生成的任务目录为`./workspace/task_1`，迁移任务配置文件路径为：`./workspace/task_1/config/migration.properties`。

请使用如下命令，前往修改迁移任务目录中的迁移任务配置文件，完成迁移任务配置

```sh
vim ./workspace/task_1/config/migration.properties
```

配置完成后，按下`ESC`键，键入`:wq`保存退出。

## 1.3 启动迁移任务

（1）启动迁移任务

启动迁移任务的命令模版如下，使用时请根据自身情况替换对应参数。

```sh
./bin/migration start <task_id>
```

其中，

- task_id：迁移任务ID，与创建迁移任务时取值一致。

命令使用示例如下

```sh
./bin/migration start 1
```

**注意：此命令启动的迁移进程为迁移主进程，迁移任务不停止，此进程会持续存活，并输出日志到终端。** 如若后台启动，可前往`./workspace/task_1/logs/portal.log`路径，查看日志文件。

（2）查看迁移任务状态

迁移任务启动成功后，可再启动一个终端，切换到portal目录下后，参考如下命令，查看迁移任务状态。

```sh
./bin/migration status 1
```

或者，使用如下命令查看迁移进度详情

```sh
./bin/migration status 1 --detail
```

（3）停止增量迁移

迁移任务包含有“增量迁移”阶段时，参考如下命令停止增量迁移。不包含增量迁移阶段时，跳过此命令。

```sh
./bin/migration stop_incremental 1
```

（4）启动反向迁移

迁移任务包含有“反向迁移”阶段时，参考如下命令启动反向迁移。不包含反向迁移阶段时，跳过此命令。

```sh
./bin/migration start_reverse 1
```

（5）停止迁移

无论迁移任务所处任何迁移阶段，均可参考如下命令停止整个迁移任务。

```sh
./bin/migration stop 1
```

停止命令执行成功后，上述迁移任务主进程不会立即退出，会进行一些清理操作后，自动退出。

（6）Tips

1. 如果一个迁移任务包含所有迁移阶段，全量迁移完成后，会自动启动全量校验，全量校验完成后，会自动启动增量迁移。增量迁移无用户干扰时，会持续进行，因此需要手动停止。手动停止增量迁移后，在手动启动反向迁移。反向迁移无用户干扰时，也会持续进行，同样需要手动停止。
2. 对于不包含所有迁移阶段的任务，各迁移阶段同样保持上述逻辑顺序，不包含的阶段会自动跳过。
3. 对于仅包含反向迁移阶段的任务，通过第一步启动迁移任务后，反向迁移阶段会自动启动，无需再手动“启动反向迁移”。
