# MapReduce-TiDB-Connector

## Build

```bash
git clone git@github.com:pingcap-incubator/TiBigData.git
cd TiBigData
mvn clean package -DskipTests -am -pl mapreduce/mapreduce-base
cp mapreduce/mapreduce-2.0/target/mapreduce-tidb-connector-base-0.0.3-SNAPSHOT.jar ${HOME}/lib
```

## Version

`hadoop 2.X` and `hadoop 3.X` are supported.


## Demo

```bash
hadoop jar mapreduce-tidb-connector-base-0.0.3-SNAPSHOT.jar io.tidb.bigdata.mapreduce.tidb.examples.TiDBMapreduceDemo  \
 -du  ${DATABASE_URL} \
 -u   ${USERNAME} \
 -p   ${PASSWORD}  \
 -dn  ${TIDB_DATABASE} \
 -n   ${TABLE_NAME} \
 -f   ${FIELD1} \
 -f   ${FIELD2} \
 -t   ${TABLE_NAME}
 -ts  ${SNAPSHOT_TIMESTAMP} \
 -l   ${LIMIT} 
```
### example

```bash
hadoop jar mapreduce-tidb-connector-base-0.0.3-SNAPSHOT.jar io.tidb.bigdata.mapreduce.tidb.example.TiDBMapreduceDemo \
 -du "jdbc:tidb://127.0.0.1:4005/pingcap" \
 -u  root \
 -p  "" \
 -dn pingcap \
 -f  filed1 \
 -f  filed2 \
 -f  filed3  \
 -f  filed4 \
 -t  tablename \
 -ts "2021-05-08T15:23:38+08:00[Asia/Shanghai]" \
 -l  3

no filed param means all fields
```

Mapper output can be found in console, like:

```bash
job attempt ID : attempt_local827968733_0001_m_000003_0
       FIELDNAME      l_orderkey       l_partkey       l_suppkey    l_linenumber
               0               1          155190            7706               1
               1               1           67310            7311               2
               2               1           63700            3701               3
```

## DataTypes

|    TiDB    |     TiResultSet       |     getMethod[TiDBResultSet]                            |   
| :--------: | :-------------------: | :-----------------------------------------------------: |   
|  TINYINT   |  TINYINT              |  getInt(columnIndex)         return int                 |   
|  SMALLINT  | SMALLINT              |  getInt(columnIndex)         return int                 |   
| MEDIUMINT  |    INT                |  getInt(columnIndex)         return int                 |   
|    INT     |    INT                |  getInt(columnIndex)         return int                 |   
|   BIGINT   |  BIGINT               |  getLong(columnIndex)        return long                |   
|    CHAR    |  STRING               |  getString(columnIndex)      return String              |   
|  VARCHAR   |  STRING               |  getString(columnIndex)      return String              |   
|  TINYTEXT  |  STRING               |  getString(columnIndex)      return String              |   
| MEDIUMTEXT |  STRING               |  getString(columnIndex)      return String              |   
|    TEXT    |  STRING               |  getString(columnIndex)      return String              |   
|  LONGTEXT  |  STRING               |  getString(columnIndex)      return String              |   
|   BINARY   |   BYTES               |  getBytes(columnIndex)       return byte[]              |   
| VARBINARY  |   BYTES               |  getBytes(columnIndex)       return byte[]              |   
|  TINYBLOB  |   BYTES               |  getBytes(columnIndex)       return byte[]              |   
| MEDIUMBLOB |   BYTES               |  getBytes(columnIndex)       return byte[]              |   
|    BLOB    |   BYTES               |  getBytes(columnIndex)       return byte[]              |   
|  LONGBLOB  |   BYTES               |  getBytes(columnIndex)       return byte[]              |   
|   FLOAT    |   FLOAT               |  getFloat(columnIndex)       return float               |   
|   DOUBLE   |  DOUBLE               |  getDouble(columnIndex)      return double              |   
| DECIMAL(p,s) |  DECIMAL(p,s)       |  getBigDecimal(columnIndex)  return BigDecimal          |   
|    DATE    |   DATE                |  getDate(columnIndex)        return java.sql.Date       |   
|    TIME    |   TIME                |  getTime(columnIndex)        return java.sql.Time       |   
|  DATETIME  | TIMESTAMP             |  getTimestamp(columnIndex)   return java.sql.Timestamp  |   
| TIMESTAMP  | TIMESTAMP             |  getTimestamp(columnIndex)   return java.sql.Timestamp  |   
|    YEAR    | SMALLINT              |  getInt(columnIndex)         return int                 |   
|    BOOL    |  BOOLEAN              |  getBoolean(columnIndex)     return boolean             |   
|    JSON    |  STRING               |  getString(columnIndex)      return String              |   
|    ENUM    |  STRING               |  getString(columnIndex)      return String              |   
|    SET     |  STRING               |  getString(columnIndex)      return String              |   
