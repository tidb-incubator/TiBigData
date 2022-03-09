# TiDB 与 Hive 集成

## 1 环境准备

| 组件    | 版本              |
|-------|-----------------|
| JDK   | 8               |
| Maven | 3.6+            |
| Hive  | 1.x / 2.x / 3.x |

Hive 版本与 TiBigData 模块对应关系：

| Hive 版本            | TiBigData 模块 |
|--------------------|--------------|
| 1.0.0 - 1.2.2      | hive-2.2.0   |
| 2.0.0 - 2.2.0      | hive-2.2.0   |
| 2.3.0 - 2.3.6      | hive-2.3.6   |
| 3.0.0 - 3.1.2      | hive-3.1.2   |

## 2 编译 Hive Storage Handler

```bash
# 克隆项目
git clone git@github.com:tidb-incubator/TiBigData.git
cd TiBigData

# 编译 Hive Storage Handler，以 hive-2.2.0 为例
mvn clean package -DskipTests -am -pl hive/hive-2.2.0
```
因为 Hive 与 Hadoop 的依赖较多，根据网络状况与电脑配置，整个过程可能需要花费 10 到 30 分钟，国内用户推荐使用国内 maven 仓库来加速。

以下是可选的编译参数：

| 参数                            | 默认值    | 描述                                                        |
|-------------------------------|--------|-----------------------------------------------------------|
| -Dmysql.driver.scope          | test   | 是否包含 mysql jdbc driver 依赖编译，可设置为 compile 以包含此依赖，默认不包含     |

## 3 安装 Hive Storage Handler

Hive 使用第三方的 Storage Handler 需要添加依赖。这里我们推荐两种方式添加 TiDB Hive Storage Handler 的 jar 包。

### 3.1 利用 ADD JAR 语法

这种做法的好处是不会给 Hive 引入额外的依赖，但是每次启动 HiveCli 或者连接到 HiveServer2 的时候都需要跑一次 ADD JAR 语法。

```bash
# 将编译出来的 jar 包上传到 hdfs 上
hadoop fs -put $TIBIGDATA_HOME/hive/hive-2.2.0/target/hive-tidb-storage-handler-2.2.0-0.0.5-SNAPSHOT.jar /tmp
# 启动 Hive 客户端或者连接至 HiveServer2 后执行如下命令初始化
ADD JAR hdfs:///tmp/hive-tidb-storage-handler-2.2.0-0.0.5-SNAPSHOT.jar;
```

### 3.2 直接将依赖放入 Hive 的 lib 目录下

```bash
cp $TIBIGDATA_HOME/hive/hive-2.2.0/target/hive-tidb-storage-handler-2.2.0-0.0.5-SNAPSHOT.jar $HIVE_HOME/lib/
```

## 4 利用 Hive 读写 TiDB
我们尝试在 TiDB 内创建一张表。

```bash
# 连接至 TiDB
mysql --host 127.0.0.1 --port 4000 -uroot --database test
```

当前**不支持**在 Hive 内向 TiDB 写数据，所以我们在 TiDB 建表并插入一条数据：

```sql
-- 这段 sql 跑在 TiDB 内
CREATE TABLE `people`(
  `id` int,
  `name` varchar(16)
);

INSERT INTO `people`(`id`,`name`) VALUES (1,'zs');
```

在 Hive 建 TiDB 的映射表，这里的字段随便写，TiBigData 会自动修正字段：

```sql
-- 如果没有将依赖拷贝进 Hive 的 lib 目录，需要使用 ADD JAR 先添加依赖
-- ADD JAR hdfs:///tmp/hive-tidb-storage-handler-2.2.0-0.0.5-SNAPSHOT.jar;
CREATE TABLE `people`(
 `fake_column` int
)
STORED BY
  'io.tidb.bigdata.hive.TiDBStorageHandler'
TBLPROPERTIES (
  'tidb.database.url'='jdbc:mysql://localhost:4000/',
  'tidb.database.name'='test',
  'tidb.table.name'='people',
  'tidb.username'='root',
  'tidb.password'=''
 );
```

在 Hive 内查看表结构：

```sql
SHOW CREATE TABLE `people`;
```

可以看到表的字段已经被自动修正了：

```sql
hive (default)> SHOW CREATE TABLE `people`;
OK
createtab_stmt
CREATE TABLE `people`(
  `id` int COMMENT 'from deserializer',
  `name` string COMMENT 'from deserializer')
ROW FORMAT SERDE
  'io.tidb.bigdata.hive.TiDBSerde'
STORED BY
  'io.tidb.bigdata.hive.TiDBStorageHandler'
WITH SERDEPROPERTIES (
  'serialization.format'='1')
LOCATION
  'hdfs://XXX:8020/user/hive/warehouse/people'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='{\"BASIC_STATS\":\"true\"}',
  'numFiles'='0',
  'numRows'='0',
  'rawDataSize'='0',
  'tidb.database.name'='test',
  'tidb.database.url'='jdbc:mysql://localhost:4000/',
  'tidb.password'='',
  'tidb.table.name'='people',
  'tidb.username'='root',
  'totalSize'='0',
  'transient_lastDdlTime'='1642779466')
Time taken: 2.276 seconds, Fetched: 23 row(s)
hive (default)>
```

在 Hive 内查询 TiDB 表：

```sql
SELECT * FROM `people`;
```

你会得到以下结果：

```sql
hive (default)> SELECT * FROM `people`;
OK
people.id	people.name
1	zs
Time taken: 4.979 seconds, Fetched: 1 row(s)
hive (default)>
```

至此，你已经知道如何在 Hive 内使用 TiBigData 了。更多高级的功能以及配置调优可参考下面的章节。

## 5 Hive 与 TiDB 的类型映射

TiDB 与 Hive 的类型映射关系可参考下表：

|     TiDB     |     Hive     |
|:------------:|:------------:|
|   TINYINT    |     INT      |
|   SMALLINT   |     INT      |
|  MEDIUMINT   |     INT      |
|     INT      |     INT      |
|    BIGINT    |    BIGINT    |
|     CHAR     |    STRING    |
|   VARCHAR    |    STRING    |
|   TINYTEXT   |    STRING    |
|  MEDIUMTEXT  |    STRING    |
|     TEXT     |    STRING    |
|   LONGTEXT   |    STRING    |
|    BINARY    |    BYTES     |
|  VARBINARY   |    BYTES     |
|   TINYBLOB   |    BYTES     |
|  MEDIUMBLOB  |    BYTES     |
|     BLOB     |    BYTES     |
|   LONGBLOB   |    BYTES     |
|    FLOAT     |    FLOAT     |
|    DOUBLE    |    DOUBLE    |
| DECIMAL(p,s) | DECIMAL(p,s) |
|     DATE     |     DATE     |
|     TIME     |    STRING    |
|   DATETIME   |  TIMESTAMP   |
|  TIMESTAMP   |  TIMESTAMP   |
|     YEAR     |     INT      |
|     BOOL     |   BOOLEAN    |
|     JSON     |    STRING    |
|     ENUM     |    STRING    |
|     SET      |    STRING    |

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
| tidb.snapshot_timestamp            | null                                                                           | TiBigData 支持读取 TiDB 的快照数据，我们采用的格式为 `java.time.format.DateTimeFormatter#ISO_ZONED_DATE_TIME`.  比如 `2021-01-01T14:00:00+08:00`                                                                                                                                                                         |
| tidb.dns.search                    | null                                                                           | TiBigData 支持在节点的域名上添加后缀来支持复杂的网络情况，比如跨数据中心的 k8s 集群。                                                                                                                                                                                                                                                   |




