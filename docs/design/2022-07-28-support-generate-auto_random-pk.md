# Support allocating auto_random primary key 

- Author(s): [yangxin](http://github.com/xuanyu66)
- Tracking Issue: https://github.com/tidb-incubator/TiBigData/issues/235

## Table of Contents
* [Support allocating auto_random primary key](#support-allocating-auto_random-primary-key)
  * [Table of Contents](#table-of-contents)
  * [Introduction](#introduction)
  * [Motivation or Background](#motivation-or-background)
  * [Detailed Design](#detailed-design)
    * [The mechanism of shard](#the-mechanism-of-shard)
    * [Get the shardBits](#get-the-shardbits)
    * [Generate the currentShard with startTimestamp](#generate-the-currentshard-with-starttimestamp)
    * [Use IGNORE_AUTO_RANDOM_COLUMN_VALUE to ignore the explicit primary key.](#use-ignore_auto_random_column_value-to-ignore-the-explicit-primary-key)
  * [Compatibility](#compatibility)
  * [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)

## Introduction

This design doc is about how to support allocating auto_random primary key in flink-tidb-connector.

## Motivation or Background

[AUTO_RANDOM](https://docs.pingcap.com/tidb/dev/auto-random) is a feature introduced by TiDB in order to increase the performance of the insert operation 
when the writing hotspot issue occurs caused by auto-increment.

The primary key, which must be bigint type and clustered, is allocated in the following way:
The highest n(default is five，1 <= n < 16) digits (ignoring the sign bit) of the row value in binary (namely, shard bits) are determined by the starting time of the current transaction. 
The remaining digits are allocated values in an auto-increment order.

Luckily, we already support auto_increment and [shard_row_id_bits](https://docs.pingcap.com/tidb/dev/shard-row-id-bits) in Flink,
of which the mechanism of the latter is similar to the auto_random, so we can reuse the methods.

## Detailed Design

Firstly, we will introduce the mechanism of shard and then discuss how we get shardBits and get the shardSeed with good collision resistance.

### The mechanism of shard

Firstly, we need to generate the shardSeed, whose bits' length must be greater than 16.
Then we take the low n bits of the shardSeed as the shard mask.

```java
long shardSeed = 0xaaaaL;
long shardBits = 6L; 
//((1L << shardBits) - 1) = 0b111111L ,shardMask = 0b101010L
long shardMask = shardSeed & ((1L << shardBits) - 1);
```

In order to shard the highest n bits, we need to left shift the shardMask by (BigintLength - shardBits - signBitLength) bits.
- The bigintLength is 64.
- The signBitLength is 0 with unsigned bigint, and 1 with signed bigint.

```text
// example with unsigned bigint
0b 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0010 1010   shardMask before left shift
                                left shift                                          
0b 1010 1000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000   shardMask after left shift
                             bit or operation
0b 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0001   auto_increment-generated-value
                                 equals
0b 1010 1000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0001   auto_random value
```

### Get the shardBits 

When the [autoRandomBits](https://github.com/tidb-incubator/TiBigData/blob/master/tidb/src/main/java/io/tidb/bigdata/tidb/meta/TiTableInfo.java#L61) greater than 0, 
the primary key of the table is auto_random.

### Generate the currentShard with startTimestamp

We use the `java.util.Random` and set the seed with the startTimestamp.
After getting random int64, we use the [MurmurHash3 algorithm](https://stackoverflow.com/questions/11899616/murmurhash-what-is-it) 
to make sure the bits of shardSeed >= 15, which is according to [TiDB code](https://github.com/pingcap/tidb/blob/1a89decdb192cbdce6a7b0020d71128bc964d30f/sessionctx/variable/session.go#L212-L216)

### Use IGNORE_AUTO_RANDOM_COLUMN_VALUE to ignore the explicit primary key.

If it is true, we will generate the primary key with the auto_random value and ignore the explicit primary key passed by the user.
If it is false, we will use the explicit primary key passed by the user, and thus, the value must not be null.

## Compatibility

- This feature works in both batch and streaming mode.
- This feature only works when `tidb.sink.impl` is `tikv`.

## Test Design

### Functional Tests
- insert into a table with auto_random bigint signed primary key with explicit value(ignore the value).
- insert into a table with auto_random bigint unsigned primary key with explicit value(ignore the value).

### Scenario Tests
The tests will run on both batch and streaming mode
- insert into a table with auto_random bigint signed primary key.
- insert into a table with auto_random bigint unsigned primary key.


