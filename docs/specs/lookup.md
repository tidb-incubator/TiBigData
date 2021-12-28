# Product Requirements Specification

Implement fast lookup via primary key or secondary indices

## Problem:

There are use cases within big-data scenarios where fast lookup with index are beneficial. 
Especially in streaming processing, lookup latency has a significant impact to overall
throughput. TiBigData implements full table scan only at this time, which makes it suboptimal to
workloads that requires low latency. Therefore, flink-tidb-connector utilize JDBC lookup table 
source instead. Since Flink, like other big-data computation frameworks, is at computing layer by
itself. Sending lookup request to TiDB server which is at computing layer as well is redundant and 
waste of resources. Lookup with TiDB server contributes to growing of latency with extra hops in
access path.

## Goals:

Implement primary key and secondary index based lookup in TiBigData. Decreases lookup latency with
the best efforts and saves unnecessary resources waste for load balancers and TiDB servers.

## Solutions:

1. Extract columns from predicates
2. Try match columns with PK and secondary indices
3. Get row handle from PK or secondary index
4. Get row data by handle or by PK if table uses clustered index

## What reports do we need:

* Latency benchmark for native point get and point get via TiDB server.
* TiDB server CPU usage.
