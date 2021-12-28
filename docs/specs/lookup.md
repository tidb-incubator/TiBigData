# Product Requirements Specification

Implement fast lookup via primary key or secondary indices

## Problem:

There are use cases within big-data scenarios where fast lookup with index are beneficial. 
Especially in streaming processing, lookup latency has a significant impact to overall
throughput. At this time, TiBigData implements full table scan only, which makes it suboptimal to
workloads that require low latency. Therefore, flink-tidb-connector utilize JDBC lookup table 
source instead. Since Flink, like other big-data computation frameworks, is at the computing layer
by itself. Sending lookup requests to TiDB server which is at computing layer as well is redundant.
Lookup with TiDB server contributes to growing of latency with extra hops in access path.

## Goals:

Implement primary key and secondary index-based lookup in TiBigData. This decreases lookup latency with
the best efforts and saves unnecessary costs for load balancers and TiDB servers.

## Solutions:

1. Extract columns from predicates.
2. Try match columns with PK and secondary indices.
3. Get handle of rows from PK or secondary index.
4. Get data by handles or by PKs if the table uses clustered index.

## What reports do we need:

* Latency benchmark for native point get and point get via TiDB server.
* TiDB server CPU usage.
