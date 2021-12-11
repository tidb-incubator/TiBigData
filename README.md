# TiBigData

[License](https://github.com/pingcap-incubator/TiBigData/blob/master/LICENSE)

Misc BigData components for TiDB, Presto, Flink and MapReduce connectors for example.

## License

TiBigData project is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

## Code style

[Google Code Style](https://github.com/google/styleguide).

## Getting Started

First you need to build the corresponding version of tikv-java-client with the following command:
```bash
./.ci/build-client-java.sh
```

[Flink-TiDB-Connector](./flink/README.md)

[PrestoSQL-TiDB-Connector](./prestosql/README.md)

[Trino-TiDB-Connector](./trino/README.md)

[PrestoDB-TiDB-Connector](./prestodb/README.md)

[MapReduce-TiDB-Connector](./mapreduce/README.md)

## Run Tests

Use the following command to run all the tests.

```
export TIDB_HOST="127.0.0.1"
export TIDB_PORT="4000"
export TIDB_USER="root"
export TIDB_PASSWORD=""
mvn test
```

![YourKit Logo](https://www.yourkit.com/images/yklogo.png)

YourKit supports open source projects with innovative and intelligent tools
for monitoring and profiling Java and .NET applications.
YourKit is the creator of <a href="https://www.yourkit.com/java/profiler/">YourKit Java Profiler</a>,
<a href="https://www.yourkit.com/.net/profiler/">YourKit .NET Profiler</a>,
and <a href="https://www.yourkit.com/youmonitor/">YourKit YouMonitor</a>.
