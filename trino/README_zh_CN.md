# TiDB 与 Trino 集成

## 1 环境准备

| 组件    | 版本   |
|-------|------|
| JDK   | 11   |
| Maven | 3.6+ |
| Trino | 359  |

## 2 编译 Trino Connector
请参考以下步骤，如注释所说，在编译之前你需要先编译 TiKV 的 java 客户端，这是因为 TiBigData 抢先用到了一些 TiKV java 客户端未发版的新功能。此外，TiBigData 的 API 基于 Trino-359 的小版本构建，如果与你的 Trino 版本不同，你需要手动将 TiBigData 依赖的 Trino 版本修改为你需要的版本，此外，你可能需要将 TiBigData 的代码进行一些小改动以兼容不同版本的 Trino。当然，你也可以按照下面的步骤重新搭建一个 350 版本的 Trino 单机版集群以做测试。

```bash
# 克隆项目
git clone git@github.com:tidb-incubator/TiBigData.git
cd TiBigData
# 在编译之前你需要先编译 TiKV 的 java 客户端
./.ci/build-client-java.sh
# 编译 Trino connector
mvn clean package -DskipTests -am -pl trino -Dmysql.driver.scope=compile
# 解压 plugin
tar -zxf trino/target/trino-connector-0.0.5-SNAPSHOT-plugin.tar.gz -C trino/target
```
因为 Trino 的依赖较多，根据网络状况与电脑配置，整个过程可能需要花费 10 到 30 分钟，国内用户推荐使用国内 maven 仓库来加速。

以下是可选的编译参数：

| 参数                            | 默认值    | 描述                                                        |
|-------------------------------|--------|-----------------------------------------------------------|
| -Dmysql.driver.scope          | test   | 是否包含 mysql jdbc driver 依赖编译，可设置为 compile 以包含此依赖，默认不包含     |

## 3 部署 Trino

Trino 提供多种部署方式，本文仅提供单机版的 Trino 部署用于测试，如果你想在生产环境使用 Trino, 请参考 [Trino 官方文档](https://trino.io)。

### 3.1 下载安装包

请到 [Trino 下载页面](https://trino.io/download.html) 下载对应版本的安装包，下载页面仅保留最新版本，历史版本可从这里找到：[Trino 历史版本](https://repo1.maven.org/maven2/io/trino/trino-server)。

### 3.2 安装 TiBigData

```bash
# 下载并解压 trino 的二进制安装包，我们以 trino-359 为例
wget https://repo1.maven.org/maven2/io/trino/trino-server/359/trino-server-359.tar.gz
# 国内用户可从国内镜像下载
# wget https://maven.aliyun.com/repository/central/io/trino/trino-server/359/trino-server-359.tar.gz
tar -zxf trino-server-359.tar.gz
# 进入到 trino 的 home 目录
cd trino-server-359
# 拷贝 plugin 到 trino 的 plugin 目录下
cp -r ${TIBIGDATA_HOME}/trino/target/trino-connector-0.0.5-SNAPSHOT/tidb plugin
```

至此，TiBigData 已经安装完成，接下来需要配置 Trino 集群，并启动。

### 3.3 配置单机版 Trino 集群

这里我们给出一份简单的配置来启动单机版的 Trino 集群。

```bash
cd $TRINO_HOME
mkdir -p etc/catalog
```

接下来是配置 Trino 集群相关的配置文件。

#### 3.3.1 配置 config.properties

```bash
vim etc/config.properties
```

`config.properties` 内容如下：

```properties
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=12345
query.max-memory=2GB
query.max-memory-per-node=2GB
query.max-total-memory-per-node=2GB
discovery-server.enabled=true
discovery.uri=http://localhost:12345
```

#### 3.3.2 配置 jvm.properties
```bash
vim etc/jvm.config
```

`jvm.config` 内容如下：

```properties
-server
-Xmx4G
-XX:+UseConcMarkSweepGC
-XX:+ExplicitGCInvokesConcurrent
-XX:+CMSClassUnloadingEnabled
-XX:+AggressiveOpts
-XX:+HeapDumpOnOutOfMemoryError
-XX:OnOutOfMemoryError=kill -9 %p
-XX:ReservedCodeCacheSize=150M
```
#### 3.3.3 配置 node.properties
```bash
vim etc/node.properties
```

`node.properties` 内容如下：

```properties
# 同一个集群内的所有节点的 environment 应该相同，否则无法进行通信
node.environment=test
# 节点的唯一标识，推荐使用 uuid 生成
node.id=1
# 日志存放目录
node.data-dir=/tmp/trino/logs
```
#### 3.3.4 配置 log.properties
```bash
vim etc/log.properties
```

`log.properties` 内容如下：

```properties
io.trino=INFO
```

#### 3.3.4 配置 tidb connector
```bash
vim etc/catalog/tidb.properties
```

`tidb.properties` 内容如下：

```properties
# 这里必须要写 tidb
connector.name=tidb
# 需要换成自己的 tidb 集群地址
tidb.database.url=jdbc:mysql://localhost:4000/test
tidb.username=root
tidb.password=
```

如果你有多个 TiDB 集群，你可以创建多个 properties 文件，比如 `tidb01.properties` 和 `tidb02.properties`，每个配置文件写不同的连接串和密码即可。

### 3.4 启动 Trino

现在你可以启动 Trino 了。

```bash
# 前台启动
bin/launcher run
# 后台启动
bin/launcher start
```

### 3.5 使用 Trino 客户端查询 TiDB 内的数据

```bash
# 下载 trino 客户端
# 国内用户可从国内镜像下载
# curl -L https://maven.aliyun.com/repository/central/io/trino/trino-cli/359/trino-cli-359-executable.jar -o trino
curl -L https://repo1.maven.org/maven2/io/trino/trino-cli/359/trino-cli-359-executable.jar -o trino
chmod 777 trino
# 连接至 trino
./trino --server localhost:12345 --catalog tidb --schema test
```

我们尝试在 TiDB 内创建一张表。

```bash
# 连接至 TiDB
mysql --host 127.0.0.1 --port 4000 -uroot --database test
```

在 TiDB 建表：

```sql
-- 这段 sql 跑在 TiDB 内
CREATE TABLE `people`(
  `id` int,
  `name` varchar(16)
);
```

建完 TiDB 的表以后，我们可以在 Trino 内查看刚刚建出来的 TiDB 的表结构：
```sql
show create table people;
```
你会得到以下下信息：

```sql
trino:test> show create table people;
          Create Table
---------------------------------
 CREATE TABLE tidb.test.people (
    id integer,
    name varchar(16)
 )
 WITH (
    primary_key = '',
    unique_key = ''
 )
(1 row)

Query 20220105_134831_00006_s5ptx, FINISHED, 1 node
Splits: 1 total, 1 done (100.00%)
0.27 [0 rows, 0B] [0 rows/s, 0B/s]
```
尝试在 Trino 内向 TiDB 插入一条数据并查询：

```sql
INSERT INTO "test"."people"("id","name") VALUES(1,'zs');
SELECT * FROM "test"."people";
```
你会得到以下信息：
```sql
trino:test> INSERT INTO "test"."people"("id","name") VALUES(1,'zs');
INSERT: 1 row

Query 20220105_135446_00004_qjyfy, FINISHED, 1 node
Splits: 35 total, 35 done (100.00%)
0.35 [0 rows, 0B] [0 rows/s, 0B/s]

trino:test> SELECT * FROM "test"."people";
 id | name
----+------
  1 | zs
(1 row)

Query 20220105_135449_00005_qjyfy, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
0.25 [1 rows, 0B] [3 rows/s, 0B/s]
```

至此，你已经知道如何在 Trino 内使用 TiBigData 了。更多高级的功能以及配置调优可参考下面的章节。

## 5 TiDB 与 Trino 的类型映射

TiDB 与 Trino 的类型映射关系可参考下表：

|     TiDB     |    Trino     |
|:------------:|:------------:|
|   TINYINT    |   TINYINT    |
|   SMALLINT   |   SMALLINT   |
|  MEDIUMINT   |     INT      |
|     INT      |     INT      |
|    BIGINT    |    BIGINT    |
|     CHAR     |   VARCHAR    |
|   VARCHAR    |   VARCHAR    |
|   TINYTEXT   |   VARCHAR    |
|  MEDIUMTEXT  |   VARCHAR    |
|     TEXT     |   VARCHAR    |
|   LONGTEXT   |   VARCHAR    |
|    BINARY    |  VARBINARY   |
|  VARBINARY   |  VARBINARY   |
|   TINYBLOB   |  VARBINARY   |
|  MEDIUMBLOB  |  VARBINARY   |
|     BLOB     |  VARBINARY   |
|   LONGBLOB   |  VARBINARY   |
|    FLOAT     |     REAL     |
|    DOUBLE    |    DOUBLE    |
|   DECIMAL    |   DECIMAL    |
|     DATE     |     DATE     |
|   TIME(p)    |   TIME(p)    |
| DATETIME(p)  | TIMESTAMP(p) |
| TIMESTAMP(p) | TIMESTAMP(p) |
|     YEAR     |   SMALLINT   |
|     BOOL     |   TINYINT    |
|     JSON     |   VARCHAR    |
|     ENUM     |   VARCHAR    |
|     SET      |   VARCHAR    |

## 6 高级配置

| Configuration                      | Default Value                                                                  | Description                                                                                                                                                                                                                                                                                          |
|:-----------------------------------|:-------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| tidb.database.url                  | -                                                                              | 你需要用 jdbc url 的格式来填写你的 TiDB server 的地址：`jdbc:mysql://host:port/database` 或者 `jdbc:tidb://host:port/database`. 如果你有多个 TiDB server，我们推荐填写后一种格式以使用 TiDB jdbc driver, TiDB jdbc driver 是 MySQL jdbc driver 的一个轻量级的包装，它会自动发现所有 TiDB server 的地址，并做负载均衡，负载均衡策略默认为随机。                                        |
| tidb.username                      | -                                                                              | 用户名。                                                                                                                                                                                                                                                                                                 |
| tidb.password                      | null                                                                           | 密码。                                                                                                                                                                                                                                                                                                  |
| tidb.jdbc.connection-provider-impl | io.tidb.bigdata.tidb.JdbcConnectionProviderFactory.BasicJdbcConnectionProvider | JDBC 连接提供方式: 设置 'io.tidb.bigdata.tidb.JdbcConnectionProviderFactory.HikariDataSourceJdbcConnectionProvider', TiBigData 将会使用连接池 [HikariCP](https://github.com/brettwooldridge/HikariCP) 提供连接; 设置 'io.tidb.bigdata.tidb.JdbcConnectionProviderFactory.BasicJdbcConnectionProvider', 将会直接新建连接，而不会用到连接池。 |
| tidb.maximum.pool.size             | 10                                                                             | 连接池大小。                                                                                                                                                                                                                                                                                               |
| tidb.minimum.idle.size             | 10                                                                             | 最小存活连接数。                                                                                                                                                                                                                                                                                             |
| tidb.write_mode                    | append                                                                         | 在向 TiDB 写入数据时指定，可指定 `upsert` 或者 `append`. 如果指定为 `append`，在写入 TiDB 时遇到唯一键约束会报错；如果指定为 `upsert` ，在写入 TiDB 时遇到唯一键约束会替换原来的数据。                                                                                                                                                                             |
| tidb.replica-read                  | leader                                                                         | TiBigData 支持从指定的角色读取数据，你配置多个角色，比如 `tidb.replica-read=leader,follower`，这代表从 leader 和 follower 读取。                                                                                                                                                                                                     |
| tidb.replica-read.label            | null                                                                           | TiBigData 支持从指定了 label 的 TiKV store 读取数据你可以这样配置：`label_x=value_x,label_y=value_y`                                                                                                                                                                                                                    |
| tidb.replica-read.whitelist        | null                                                                           | TiKV store 的 ip 白名单列表，如果配置了，TiBigData 将会只从这些节点读取数据。                                                                                                                                                                                                                                                  |
| tidb.replica-read.blacklist        | null                                                                           | TiKV store 的 ip 黑名单列表，如果配置了，TiBigData 将不会从这些节点读取数据。                                                                                                                                                                                                                                                  |
| tidb.snapshot_timestamp            | null                                                                           | TiBigData 支持读取 TiDB 的快照数据，我们采用的格式为 `java.time.format.DateTimeFormatter#ISO_ZONED_DATE_TIME`.  你可以设置 session 变量来读取快照： `SET SESSION tidb.snapshot_timestamp='2021-01-01T14:00:00+08:00'` ，或者取消 session 变量来禁用： `SET SESSION tidb.snapshot_timestamp=''` .                                               |
| tidb.dns.search                    | null                                                                           | TiBigData 支持在节点的域名上添加后缀来支持复杂的网络情况，比如跨数据中心的 k8s 集群。                                                                                                                                                                                                                                                   |
