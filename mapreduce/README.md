# MapReduce-TiDB-Connector

## Build

```bash
git clone git@github.com:pingcap-incubator/TiBigData.git
cd TiBigData
mvn clean package -DskipTests -am -pl mapreduce/mapreduce-base
cp  mapreduce/mapreduce-base/target/mapreduce-tidb-connector-base-0.0.4-SNAPSHOT.jar ${HOME}/lib
```

## Version

`hadoop 2.X` and `hadoop 3.X` are supported.

## Demo

```bash
hadoop jar mapreduce-tidb-connector-base-0.0.4-SNAPSHOT.jar io.tidb.bigdata.mapreduce.tidb.example.TiDBMapreduceDemo  \
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

### create table and insert record

you could use mysql client to create the table and insert the record:

```sql
CREATE TABLE IF NOT EXISTS test.test_table
    (
        c1  tinyint,
        c2  smallint,
        c3  mediumint,
        c4  int,
        c5  bigint,
        c6  char(10),
        c7  varchar(20),
        c8  tinytext,
        c9  mediumtext,
        c10 text,
        c11 longtext,
        c12 binary(20),
        c13 varbinary(20),
        c14 tinyblob,
        c15 mediumblob,
        c16 blob,
        c17 longblob,
        c18 float,
        c19 double,
        c20 decimal(6, 3),
        c21 date,
        c22 time,
        c23 datetime,
        c24 timestamp,
        c25 year,
        c26 boolean,
        c27 json,
        c28 enum ('1','2','3'),
        c29 set ('a','b','c'),
        PRIMARY KEY(c1),
        UNIQUE KEY(c2)
    );
    
INSERT INTO test.test_table VALUES                                                                                                                                                                                                                                                                                                                                                                                                              
    (
        127,32767,
        8388607,
        2147483647,
        9223372036854775807,
        "chartype",
        "varchartype",
        "tinytexttype",
        "mediumtexttype",
        "texttype",
        "longtexttype",
        "binarytype",
        "varbinarytype",
        "tinyblobtype",
        "mediumblobtype",
        "blobtype",
        "longblobtype",
        1.324235235345,
        2.123423423423,
        999.999,
        "9999-12-31",
        "23:59:59",
        "9999-12-31 23:59:59",
        "2038-01-19 03:14:07",
        "2155",
        0,
        '{"a": 1, "b": 2}',
        "1","a"
    );
```

### params

| param_name                | require |     example                                                                     |   
| :-----------------------: | :-----: | :-----------------------------------------------------------------------------: | 
| filed_name, -f            | false   |  -f f1 -f f2(select f1, f2 from ...), Not having this param(select * from ...)  |  
| database_url, -du         | true    |  -du "jdbc:mysql://127.0.0.1:4005/pingcap"                                      |   
| username, -u              | true    |  -u root                                                                        |   
| password, -p              | true    |  -p ""                                                                          |   
| database_name, -dn        | true    |  -dn test                                                                       |   
| table_name, -t            | true    |  -t test_table                                                                  |   
| snapshot_timestamp, -ts   | false   |  -ts "2021-05-08T15:23:38+08:00[Asia/Shanghai]"                                 |   
| limit, -l                 | false   |  -l 3                                                                           |

### example

```bash
hadoop jar mapreduce-tidb-connector-base-0.0.3-SNAPSHOT.jar io.tidb.bigdata.mapreduce.tidb.example.TiDBMapreduceDemo \
 -du "jdbc:mysql://127.0.0.1:4005/test" \
 -f c1 -f c2 -f c3 -f c4 -f c5 -f c6 -f c7 -f c8 -f c9 -f c10 -f c11 \
 -f c12 -f c13 -f c14 -f c15 -f c16 -f c17 -f c18 -f c19 -f c20 -f c21 \
 -f c22 -f c23 -f c24 -f c25 -f c26 -f c27 -f c28 -f c29 \
 -u  root \
 -p  "" \
 -dn test \
 -t  test_table \
 -ts "2021-05-08T15:23:38+08:00[Asia/Shanghai]" \
 -l  3

note: no filed param means all fields
```

Mapper output can be found in console, like:

```bash
job attempt ID : attempt_local827968733_0001_m_000003_0
       FIELDNAME      c1              c2              c3              c4        
               0       1          155190            7706               1
               1       1           67310            7311               2
               2       1           63700            3701               3
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
