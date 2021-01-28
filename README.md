# TiBigData

[License](https://github.com/pingcap-incubator/TiBigData/blob/master/LICENSE)

Misc BigData components for TiDB, Presto & Flink connectors for example.

## License

TiBigData project is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

## Code style

[Google Code Style](https://github.com/google/styleguide).

## Getting Started

[Flink-TiDB-Connector](./flink/README.md)

[PrestoSQL-TiDB-Connector](./prestosql/README.md)

[PrestoDB-TiDB-Connector](./prestodb/README.md)

## Run Tests

Use the following command to run all the tests.

```
TIDB_URL="jdbc:tidb://172.0.0.1:4000?user=root&password="
mvn test
```
