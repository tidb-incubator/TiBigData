# Flink 下推说明

从 Flink 1.13 开始我们提供了 Flink 到 TiDB 的下推支持，下面是一些说明。

当遇到不支持的算子时，你并不会读取到错误的数据，而是会读取到更多的数据，Flink 会帮你进行二次过滤，确保读到的数据是你想要的。

## 二元运算符

###  '=' '>' '<' '>=' '<=' '<>'

以 '=' 为例，支持下推的方式有：

`c1 = xxx` 以及 `c1 = CAST(xxx AS INT)`，而 `CAST(c1 AS INT) = xxx` 将会被简化为 `c1 = xxx`。

`c1 = c2` 不支持。

支持的数据类型如下：

|     TiDB     |    Flink     | Supported |
|:------------:|:------------:|:---------:|
|   TINYINT    |   TINYINT    |   TRUE    |
|   SMALLINT   |   SMALLINT   |   TRUE    |
|  MEDIUMINT   |     INT      |   TRUE    |
|     INT      |     INT      |   TRUE    |
|    BIGINT    |    BIGINT    |   TRUE    |
|     CHAR     |    STRING    |   TRUE    |
|   VARCHAR    |    STRING    |   TRUE    |
|   TINYTEXT   |    STRING    |   TRUE    |
|  MEDIUMTEXT  |    STRING    |   TRUE    |
|     TEXT     |    STRING    |   TRUE    |
|   LONGTEXT   |    STRING    |   TRUE    |
|    BINARY    |    BYTES     |   FALSE   |
|  VARBINARY   |    BYTES     |   FALSE   |
|   TINYBLOB   |    BYTES     |   FALSE   |
|  MEDIUMBLOB  |    BYTES     |   FALSE   |
|     BLOB     |    BYTES     |   FALSE   |
|   LONGBLOB   |    BYTES     |   FALSE   |
|    FLOAT     |    FLOAT     |   TRUE    |
|    DOUBLE    |    DOUBLE    |   TRUE    |
| DECIMAL(p,s) | DECIMAL(p,s) |   TRUE    |
|     DATE     |     DATE     |   TRUE    |
|     TIME     |     TIME     |   TRUE    |
|   DATETIME   |  TIMESTAMP   |   TRUE    |
|  TIMESTAMP   |  TIMESTAMP   |   TRUE    |
|     YEAR     |   SMALLINT   |   TRUE    |
|     BOOL     |   BOOLEAN    |   TRUE    |
|     JSON     |    STRING    |   FALSE   |
|     ENUM     |    STRING    |   TRUE    |
|     SET      |    STRING    |   FALSE   |

### 'LIKE'

只有 `c1 LIKE 'xxx'` 和 `c1 LIKE CAST(xxx AS STRING)` 此类语法支持，列 `c1` 必须为字符串类型。

## 'IS NULL' AND 'NOT NULL'

支持所有数据类型。

## 'AND'

语法为 `c1 = 1 AND c2 = 1`，当 `c1 = 1` 不支持时，将会被简化为 `c2 = 1`。

## 'OR'

语法为 `c1 = 1 OR c2 = 1`，当 `c1 = 1` 不支持时，将会被简化为全表扫描。



