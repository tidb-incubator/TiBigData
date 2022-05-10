# Flink Filter Push Down Description

Starting from Flink 1.13, we have provided the push down support from Flink to tidb. Here are some instructions.

When encountering unsupported operators, you will not read the wrong data, but will read more data. Flink will help you conduct secondary filtering to ensure that the data you read is what you want.

## Binary operator

###  '=' '>' '<' '>=' '<=' '<>'

Taking '=' as an example: 

`c1 = xxx` and `c1 = CAST(xxx AS INT)` are supported, and `CAST(c1 AS INT) = xxx` will be simplified to `c1 = xxx`.

`c1 = c2` is not supported.

The supported data types are as follows：

|     TiDB     |    Flink     |      Supported      |
|:------------:|:------------:|:-------------------:|
|   TINYINT    |   TINYINT    |        TRUE         |
|   SMALLINT   |   SMALLINT   |        TRUE         |
|  MEDIUMINT   |     INT      |        TRUE         |
|     INT      |     INT      |        TRUE         |
|    BIGINT    |    BIGINT    |        TRUE         |
|     CHAR     |    STRING    |        TRUE         |
|   VARCHAR    |    STRING    |        TRUE         |
|   TINYTEXT   |    STRING    |        TRUE         |
|  MEDIUMTEXT  |    STRING    |        TRUE         |
|     TEXT     |    STRING    |        TRUE         |
|   LONGTEXT   |    STRING    |        TRUE         |
|    BINARY    |    BYTES     |        FALSE        |
|  VARBINARY   |    BYTES     |        FALSE        |
|   TINYBLOB   |    BYTES     |        FALSE        |
|  MEDIUMBLOB  |    BYTES     |        FALSE        |
|     BLOB     |    BYTES     |        FALSE        |
|   LONGBLOB   |    BYTES     |        FALSE        |
|    FLOAT     |    FLOAT     |        TRUE         |
|    DOUBLE    |    DOUBLE    |        TRUE         |
| DECIMAL(p,s) | DECIMAL(p,s) |        TRUE         |
|     DATE     |     DATE     |        TRUE         |
|     TIME     |     TIME     |        TRUE         |
|   DATETIME   |  TIMESTAMP   |        TRUE         |
|  TIMESTAMP   |  TIMESTAMP   |        TRUE         |
|     YEAR     |   SMALLINT   |        TRUE         |
|     BOOL     |   BOOLEAN    |        TRUE         |
|     JSON     |    STRING    |        FALSE        |
|     ENUM     |    STRING    | TRUE(TiKV >= 5.1.0) |
|     SET      |    STRING    |        FALSE        |

### 'LIKE'

Only `c1 LIKE 'xxx'` and `c1 LIKE CAST(xxx AS STRING)` are supported，column `c1` must be string.

## 'IS NULL' AND 'NOT NULL'

All types are supported.

## 'AND'

`${OPERATOR1} AND ${OPERATOR2}` is supported, when `${OPERATOR1}` is not supported，whole filter will be simplified to `${OPERATOR2}`.

## 'OR'

`${OPERATOR1} OR ${OPERATOR2}`  is supported, when `${OPERATOR1}` or `${OPERATOR2}` is not supported，we will scan the whole table.



