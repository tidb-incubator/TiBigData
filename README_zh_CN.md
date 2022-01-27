# TiBigData
[License](https://github.com/pingcap-incubator/TiBigData/blob/master/LICENSE)

---
[![EN doc](https://img.shields.io/badge/document-English-blue.svg)](README.md)
[![CN doc](https://img.shields.io/badge/文档-中文版-blue.svg)](README_zh_CN.md)
---

TiBigData 是为了整合 TiDB 与大数据而诞生的项目，它借助 Flink/Presto/MapReduce 等分布式计算框架，充分发挥 TiDB 分布式集群的优势，为 TiDB 在大数据场景下的使用带来良好的用户体验。

## License

TiBigData project is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

## 代码风格

[Google Code Style](https://github.com/google/styleguide).

## 快速开始

[TiDB 与 Flink 集成](./flink/README_zh_CN.md)

[TiDB 与 PrestoSQL 集成 ***- 已废弃***](./prestosql/README_zh_CN.md)

[TiDB 与 Trino 集成](./trino/README_zh_CN.md)

[TiDB 与 PrestoDB 集成](./prestodb/README_zh_CN.md)

[TiDB 与 MapReduce 集成](./mapreduce/README_zh_CN.md)

[TiDB 与 Hive 集成](./hive/README_zh_CN.md)

## 运行测试

你可以使用下面的命令来运行集成测试，注意将地址、用户名、密码等参数换成自己真实的。

```
export TIDB_HOST="127.0.0.1"
export TIDB_PORT="4000"
export TIDB_USER="root"
export TIDB_PASSWORD=""
mvn test
```

## 社区

Lark / Feishu Group:

<img src="docs/assets/lark_group.png" width="300">

## 致谢

### YourKit

![YourKit Logo](https://www.yourkit.com/images/yklogo.png)

YourKit supports open source projects with innovative and intelligent tools
for monitoring and profiling Java and .NET applications.
YourKit is the creator of <a href="https://www.yourkit.com/java/profiler/">YourKit Java Profiler</a>,
<a href="https://www.yourkit.com/.net/profiler/">YourKit .NET Profiler</a>,
and <a href="https://www.yourkit.com/youmonitor/">YourKit YouMonitor</a>.

### IntelliJ IDEA

<img src="https://resources.jetbrains.com/storage/products/company/brand/logos/IntelliJ_IDEA_icon.png" width="50">

[IntelliJ IDEA](https://www.jetbrains.com/?from=TiBigData) is a Java integrated development environment (IDE) for developing computer software.  
It is developed by JetBrains (formerly known as IntelliJ), and is available as an Apache 2 Licensed community edition,  
and in a proprietary commercial edition. Both can be used for commercial development.  
