# One-Click MySQL Migration

## Function Overview

`gs_rep_portal` is a Linux-based Java tool that integrates full and incremental migration, reverse migration, and data checks. It enables one-click installation and task configuration, allowing you to orchestrate migration workflows via custom execution plans. The system invokes the appropriate tools for each step, providing real-time visibility into status, progress, and error diagnostics.

## Precautions

- The portal requires curl for incremental migration, reverse migration, and data checks.
- Incremental and reverse migration cannot run simultaneously within the same plan. If a plan includes both, you must manually stop the incremental migration before starting the reverse migration. reverting back to incremental migration is not supported.
- The `workspace.id` must contain only lowercase letters and digits.
- When running multiple plans, ensure unique source (MySQL) and target (openGauss) database pairs. Concurrent incremental and reverse migration is prohibited for the same database pair.

 ## Default File Structure

The file structure of the portal installed using the default configuration is as follows:

   ```
portal/
	config/    
		migrationConfig.properties
		toolspath.properties
		status
		currentPlan
		input
		chameleon/
			config-example.yml
		datacheck/
			application-source.yml
			application-sink.yml
			application.yml
			log4j2.xml
			log4j2source.xml
			log4j2sink.xml
		debezium/
			connect-avro-standalone.properties
			mysql-sink.properties
			mysql-source.properties
			opengauss-sink.properties
			opengauss-source.properties
	logs/      
		portal.log 
	pkg/           
		chameleon/
			chameleon-7.0.0rc3-py3-none-any.whl
		datacheck/
			openGauss-DataCheck-7.0.0-RC3.tar.gz
		debezium/
			confluent-community-5.5.1-2.12.zip
			openGauss-IncReplicateMysql2OpenGauss-7.0.0-RC3.tar.gz
			openGauss-IncReplicateOpenGauss2Mysql-7.0.0-RC3.tar.gz
	tmp/
	tools/
		chameleon/
		datacheck/
		debezium/
			confluent-5.5.1/
			plugin/
				debezium-connector-mysql/
				debezium-connector-opengauss/
	portal.portId.lock
	portalControl-7.0.0-RC3-exec.jar
	gs_datacheck.sh
	gs_mysync.sh
	gs_rep_portal.sh
	gs_replicate.sh
	README.md
   ```

## Installation

The default portal installation directory is `/ops/portal`. You can change it as required.

### Installation Using Source Code

1. Run the git command to download the source code. Then, copy the `portal` folder in the source code and paste it to the `/ops` directory.

```
git clone https://gitee.com/opengauss/openGauss-migration-portal.git
```

2. Run the Maven command to compile the source code and obtain the `portalControl-7.0.0-RC3-exec.jar` file. Then, place the JAR file in the `/ops/portal` directory.

```
mvn clean package -Dmaven.test.skip=true
```

Java version: JDK 17 or later

Maven version: 3.8.1 or later

3. When using the one-click script to start the portal, extract the `.sh` file from the `/ops/portal/shell` directory and place it in the `/ops/portal/` directory, that is, the directory where the JAR package is stored.

### Installation Using a Package

The download links for each system version and architecture are as follows.

| System Name          | System Architecture| Download Link                                                                                                                |
|:---------------| -------- |----------------------------------------------------------------------------------------------------------------------|
| CentOS 7       | x86_64  | https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/centos7/PortalControl-7.0.0-RC3-x86_64.tar.gz        |
| openEuler 20.03| x86_64  | https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/openEuler20.03/PortalControl-7.0.0-RC3-x86_64.tar.gz  |
| openEuler 20.03| AArch64 | https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/openEuler20.03/PortalControl-7.0.0-RC3-aarch64.tar.gz |
| openEuler 22.03| x86_64  | https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/openEuler22.03/PortalControl-7.0.0-RC3-x86_64.tar.gz  |
| openEuler 22.03| AArch64 | https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/openEuler22.03/PortalControl-7.0.0-RC3-aarch64.tar.gz |
| openEuler 24.03| x86_64  | https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/openEuler24.03/PortalControl-7.0.0-RC3-x86_64.tar.gz  |
| openEuler 24.03| AArch64 | https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/openEuler24.03/PortalControl-7.0.0-RC3-aarch64.tar.gz |

1. Download the `gs_rep_portal` installation package.

   ```
wget -c https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/centos7/PortalControl-7.0.0-RC3-x86_64.tar.gz
   ```

2. Decompress the `gs_rep_portal` installation package.

   ```
tar -zxvf PortalControl-7.0.0-RC3-x86_64.tar.gz
   ```

## Startup Method

Use the `gs_rep_portal` script to manage the portal via command-line parameters.

   ```
sh gs_rep_portal.sh Parameter workspace.id &
   ```

Parameters are underscore-separated strings (e.g., `start_mysql_full_migration`) representing commands for installation, startup, stopping, or uninstallation, as detailed in the following sections.

The portal creates a workspace directory named after the `workspace.id` to store task parameters and logs. If no ID is specified, it defaults to 1.

To check usage instructions and available commands, run:

   ```
sh gs_rep_portal.sh help &
   ```

Parameter priority: Workspace-specific settings override public space settings. If the provided `workspace.id` matches an existing workspace, the system reuses the existing parameters. If it is new, the portal initializes the workspace by copying the default configuration from the `config` folder.

It is recommended to use a unique `workspace.id` for each migration task to ensure configuration isolation.

### Installing the Migration Tool

The following table lists the migration functions and corresponding migration tools.

| Migration Function                          | Tool                               |
| ---------------------------------- | --------------------------------------- |
| Full migration                          | Chameleon                              |
| Incremental migration                          | confluent, debezium-connector-mysql    |
| Reverse migration                          | confluent, debezium-connector-opengauss|
| Data check (including full check and incremental check)| confluent, datacheck                   |

The following table lists the version requirements.

| Tool                         | Version      |
|-----------------------------|----------|
| Chameleon                  | 7.0.0rc3 |
| confluent                   | 5.5.1    |
| datacheck                  | 7.0.0-RC3 |
| replicate-mysql2openGauss   | 7.0.0-RC3 |
| replicate-openGauss2mysql   | 7.0.0-RC3 |

Run the following command to install all migration tools:

```sh
sh gs_rep_portal.sh install_mysql_all_migration_tools 1
```

#### Preparations

If additional tools are installed, the portal automatically starts the built-in Confluent (Kafka) service as a prerequisite. This startup process is triggered immediately upon completion of the installation.

Command for ending the preparation action:

`sh gs_rep_portal.sh stop_kafka a`

Command for starting the preparation action:

`sh gs_rep_portal.sh start_kafka a`

#### Offline Installation

Chameleon, the full migration tool written in Python, requires specific development libraries (`mariadb-devel`/`mysql-devel`/`mysql5-devel`, `python-devel`, and `python3-devel`) to function. In offline environments where these dependencies are missing and Internet access is unavailable, standard installation will fail.

To resolve this and optimize the user experience, the portal now bundles these dependencies. For offline installations, you simply need to grant the installation user password-less `sudo` privileges. The portal will automatically install the required system libraries before deploying Chameleon. Once the installation is complete, you may revoke the `sudo` permission, as it is not required for subsequent migration tasks.

Download the portal installation package matching your system architecture from the link provided above to utilize this feature.

Note that granting `sudo` permission is optional. If your environment already contains the necessary dependencies, Chameleon can be installed successfully without the privileges.

### Configuring Parameters

You can modify migration parameters in the `migrationConfig.properties` file in the `/ops/portal/config` directory.

Parameter priority: Workspace-specific settings override public space settings. If the provided `workspace.id` matches an existing workspace, the system reuses the existing parameters. If it is new, the portal initializes the workspace by copying the default configuration from the `config` folder.

| Parameter                 | Description               |
| ------------------------- | ----------------------- |
| mysql.user.name           | MySQL database username      |
| mysql.user.password       | MySQL database user password    |
| mysql.database.host       | IP address of the MySQL database          |
| mysql.database.port       | MySQL database port        |
| mysql.database.name       | MySQL database name          |
| opengauss.user.name       | openGauss database username  |
| opengauss.user.password   | openGauss database user password|
| opengauss.database.host   | IP address of the openGauss database      |
| opengauss.database.port   | openGauss database port    |
| opengauss.database.name   | openGauss database name      |
| opengauss.database.schema | openGauss database schema name  |

In addition to basic migration parameters, you can configure tool-specific settings in the designated directories. By default, the portal manages temporary files, log locations, and port allocation for these tools. You may inspect and modify the tool configuration files listed in the table below.

Precautions:

- The default ZooKeeper port 2181, Kafka port 9092, and schema-registry port 8081 are not automatically allocated. Other tools automatically allocate ports. If you need to change the port number of the tool, do not change the IP address. If you need to change the Kafka port, change the value of `listeners` in the Kafka file to `PLAINTEXT://localhost:Port_to_be_configured`.
- In the following table, `${config}` indicates the `/ops/portal/config` directory, that is, the parameter configured for the public space. If you want to modify the parameter of a workspace, for example, the parameters of the plan whose `workspace.id` is `2`, replace `/ops/portal/config` with `/ops/portal/workspace/2/config`.
- In the following table, `${confluent.path}` indicates the value of `confluent.path` in the `toolspath.properties` file in the `/ops/portal/config` directory.
- Each time a task is created, the `connect-avro-standalone.properties` file in the `/ops/portal/config/debezium` directory is duplicated four times with updated port numbers.

<table>
	<tr>
		<td>Tool</td>
		<td>Location of the Configuration File</td>
	</tr>
	<tr>
		<td>Chameleon</td>   
		<td>${config}/chameleon/config-example.yml</td>  
	</tr>
	<tr>
		<td>Zookeeper</td>   
		<td>${confluent.path}/etc/kafka/zookeeper.properties</td>  
	</tr>
	<tr>
		<td>Kafka</td>   
		<td>${confluent.path}/etc/kafka/server.properties</td>
	</tr>
	<tr>
		<td>schema-registry</td>    
		<td>${confluent.path}/etc/schema-registry/schema-registry.properties</td>
	</tr>
    <tr>
    	<td rowspan="3">connector-mysql</td>
		<td>${config}/debezium/connect-avro-standalone.properties</td>  
	</tr>
	<tr>
		<td>${config}/debezium/mysql-source.properties</td>  
	</tr>
	<tr>
		<td>${config}/debezium/mysql-sink.properties</td>  
	</tr>
    <tr>
    	<td rowspan="3">connector-opengauss</td>
		<td>${config}/debezium/connect-avro-standalone.properties</td>  
	</tr>
	<tr>
		<td>${config}/debezium/opengauss-source.properties</td>  
	</tr>
	<tr>
		<td>${config}/debezium/opengauss-sink.properties</td>  
	</tr>
    <tr>
    	<td rowspan="3">datacheck</td>
		<td>${config}/datacheck/application-source.yml</td>  
	</tr>
	<tr>
		<td>${config}/datacheck/application-sink.yml </td>  
	</tr>
	<tr>
		<td>${config}/datacheck/application.yml </td>  
	</tr>
</table>

## Executing a Migration Plan

The portal allows you to start multiple tasks to execute different migration plans. However, the MySQL instance and openGauss database used by each migration plan must be different.

When starting a migration plan, you need to add parameters so that different migration plans can be distinguished by `workspace.id`. If you do not add parameters, the default value of `workspace.id` is `1`.

Run the following command to start a full migration plan whose `workspace.id` is `2`.

   ```
sh gs_rep_portal.sh start_mysql_full_migration 2 &
   ```

In addition to starting and stopping a single function, the portal also provides some combined default plans.

For example, the following command starts a migration plan whose `workspace.id` is `2`. It contains full migration and full data check.

   ```
sh gs_rep_portal.sh start_plan1 2 &
   ```

### Plan List

| Plan| Command                                    |
| -------- | -------------------------------------------- |
| plan1    | Full migration → Full data check                           |
| plan2    | Full migration → Full data check → Incremental migration → Incremental data check         |
| plan3    | Full migration → Full data check → Incremental migration → Incremental data check → Reverse migration|

### Incremental and Reverse Migrations

The incremental migration function continuously synchronizes data modifications from MySQL to openGauss, while the reverse migration function synchronizes changes from openGauss back to MySQL. Since these functions operate independently, they are not automatically disabled. To stop either process, you must open a separate terminal window and execute the corresponding stop command.

Incremental and reverse migration cannot run simultaneously. If a plan includes both, you must manually stop the incremental migration before starting the reverse migration. Do not perform any operations on the openGauss database after stopping incremental migration and before starting reverse migration. Doing so will result in data loss.

The following uses `plan3` as an example:

1. After setting the configuration file, run the following command to start `plan3` whose `workspace.id` is `3`.

   ```
sh gs_rep_portal.sh start_plan3 3 &
   ```

The portal automatically performs full migration, full data check, incremental migration, and incremental data check. Then, the portal remains in the incremental migration state, during which both incremental migration and incremental data check run concurrently.

2. If you want to stop incremental migration, open another window and run the following command.

   ```
sh gs_rep_portal.sh stop_incremental_migration 3 &
   ```

After the command is executed, the process exits. The main portal process (`workspace.id`: `3`), which is currently executing the plan, receives a stop signal for incremental migration. It then halts the incremental migration and waits for further instructions.

3. To enable the reverse migration function, run the following command.

   ```
sh gs_rep_portal.sh run_reverse_migration 3 &
   ```

After the command is executed, the process exits. The main portal process (`workspace.id`: `3`), which is currently executing the plan, receives a start signal for reverse migration. It then initiates the reverse migration and remains in the reverse migration state.

If you want to stop the entire migration plan, see section "Stopping a Plan."

The following table lists the commands for starting a migration plan.

### Startup Command List

| Command                                       | Description                                                |
|---------------------------------------------|-------------------------------------------------             |
| verify_pre_migration                        | Pre-migration verification                                                  |
| verify_reverse_migration                    | Pre-reverse migration verification                                              |
| start_mysql_full_migration                  | Starting the MySQL full migration                                           |
| start_mysql_incremental_migration           | Starting the MySQL incremental migration                                           |
| start_mysql_reverse_migration               | Starting the MySQL reverse migration                                           |
| start_mysql_full_migration_datacheck        | Starting the MySQL full data check                                           |
| start_mysql_incremental_migration_datacheck | Starting the MySQL incremental data check                                           |
| start_plan1                                 | Starting the default plan `plan1`                                           |
| start_plan2                                 | Starting the default plan `plan2`                                           |
| start_plan3                                 | Starting the default plan `plan3`                                           |
| start_current_plan                          | Starting a custom plan                                              |
| show_plans                                  | Displaying the default plan                                                |
| show_information                            | Displaying database information, including the database names, usernames, passwords, IP addresses, and ports of MySQL and openGauss|
| stop_plan                                   | Stopping a plan                                                    |

You can also customize a migration plan in the `currentPlan` file in the `/ops/portal/config` directory. However, the customized migration plan must comply with the following rules:

- Enter a command for starting a single migration task in each line of the `currentPlan` file, for example, `start_mysql_full_migration` and `start_mysql_incremental_migration`. The sequence of commands is as follows:

	- start_mysql_full_migration
	- start_mysql_full_migration_datacheck
	- start_mysql_incremental_migration
	- start_mysql_incremental_migration_datacheck
	- start_mysql_reverse_migration

    If the sequence is incorrect, the portal reports an error.

- The previous task of incremental data check must be incremental migration, and the previous task of full data check must be full migration.

- Each single task can be added only once.

### Stopping a Plan

Example:

A task with `workspace.id` being `3` is running on the portal. Open another window and run the following command to stop the task:

   ```
sh gs_rep_portal.sh stop_plan 3 &
   ```

After the command is executed, the process exits. The main portal process (`workspace.id`: `3`), which is currently executing the plan, receives a stop signal and then stops the plan.

### Starting Multiple Plans

Multiple plans can be started on the portal at the same time. However, plans on the MySQL end must be different instances, and those on the openGauss end must be different databases.

Modify the configuration file. For details, see "Configuring Parameters".

Start the first migration plan `plan3` whose `workspace.id` is `p1`.

   ```
sh gs_rep_portal.sh start_plan3 p1 &
   ```

Then, modify the configuration file again.

Start the first migration plan `plan3` whose `workspace.id` is `p2`.

   ```
sh gs_rep_portal.sh start_plan3 p2 &
   ```

In this way, multiple plans are started on the portal.

## Uninstalling Migration Tools

Run the following commands to uninstall migration tools.

   ```
sh gs_rep_portal.sh uninstall_mysql_all_migration_tools 1 &
   ```

Run the following commands in the CLI to uninstall migration tools.

| Command                                   | Description             |
| ------------------------------------------- | --------------------- |
| uninstall_mysql_full_migration_tools        | Uninstalling the MySQL full migration tool|
| uninstall_mysql_incremental_migration_tools | Uninstalling the MySQL incremental migration tool|
| uninstall_mysql_datacheck_tools             | Uninstalling the MySQL data check tool|
| uninstall_mysql_reverse_migration_tools     | Uninstalling the MySQL reverse migration tool|
| uninstall_mysql_all_migration_tools         | Uninstalling the MySQL migration tool    |

## Data Migration Process

1. Download the `gs_rep_portal` installation package.

   ```
wget -c https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/centos7/PortalControl-7.0.0-RC3-x86_64.tar.gz
   ```

2. Decompress the `gs_rep_portal` installation package.

   ```
tar -zxvf PortalControl-7.0.0-RC3-x86_64.tar.gz
   ```

3. Change the installation path in the `toolspath.properties` file in the `/ops/portal/config` directory and run the following command to start the installation.

   ```
sh gs_rep_portal.sh install_mysql_all_migration_tools 1 &
   ```

4. Modify the migration parameters in the `migrationConfig.properties` file in the `/ops/portal/config` directory, set `workspace.id` to `2`, and start the migration plan `plan3`.

   ```
sh gs_rep_portal.sh start_plan3 2 &
   ```

5. The program automatically runs until both incremental migration and incremental data check are enabled. The task whose `workspace.id` is `2` stops incremental migration. At this point, the program enters the waiting state. Then, you can start reverse migration or stop the plan.

   ```
sh gs_rep_portal.sh stop_incremental_migration 2 &
   ```

6. Start reverse migration. The program enters the reverse migration state. Then, you can stop the plan.

   ```
sh gs_rep_portal.sh run_reverse_migration 2 &
   ```

7. Stop the plan whose `workspace.id` is `2`.

   ```
sh gs_rep_portal.sh stop_plan 2 &
   ```
#### Contributions

1. Fork this repository.
2. Create a Feat_xxx branch.
3. Commit the code.
4. Create a pull request.


#### References

1. Use the file naming pattern `README\_xx.md` to indicate a supported language (for example, `README\_en.md`).
2. Gitee blogs: [blog.gitee.com](https://blog.gitee.com)
3. Excellent open-source projects on Gitee: [https://gitee.com/explore](https://gitee.com/explore)
4. Most valuable projects on Gitee: [GVP](https://gitee.com/gvp)
5. User manual provided by Gitee: [https://gitee.com/help](https://gitee.com/help)
6. Gitee Stars, a column showcasing outstanding members of Gitee: [https://gitee.com/gitee-stars/](https://gitee.com/gitee-stars/)
