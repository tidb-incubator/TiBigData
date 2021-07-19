# TiBigData

[License](https://github.com/pingcap-incubator/TiBigData/blob/master/LICENSE)

Misc BigData components for TiDB, Presto & Flink connectors for example.

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

[PrestoDB-TiDB-Connector](./prestodb/README.md)

## Run Tests

Use the following command to run all the tests.

```
export TIDB_HOST="127.0.0.1"
export TIDB_PORT="4000"
export TIDB_USER="root"
export TIDB_PASSWORD=""
mvn test
```
