# TiDB Design Documents

- Author(s): [Mengyu Hu](https://github.com/humengyu2012), [Xiaoguang Sun](https://github.com/sunxiaoguang)

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

由知乎发起的 incubator 项目 TiBigData，旨在解决企业大数据场景中各项技术栈对接 TiDB 的整合问题。目前已提供 TiDB 与 Flink 和 Presto 的整合能力，并已应用在知乎的数据集成平台和 OLAP 分析场景中。TiBigData 项目属于整个 TiDB 社区，一方面希望能够惠及社区内有类似需求的用户，另一方面也希望发动社区的力量补全 TiDB 在大数据各项技术栈的支持能力，将 TiBigData 打造为 TiDB 在大数据领域的一站式解决方案。

## Motivation or Background

TiDB 作为目前最流行的数据库之一，被广泛应用于各大知名公司的生产中。TiDB 相比于其他单机数据库，有着更优秀的性能，比如支持更高的吞吐，支持分布式事务的读取和写入等。因此，不少企业级用户将 TiDB 作为主要的数据源，应用于复杂的大数据场景中。但是在 TiDB 作为数据源与其他大数据计算框架结合时，尚未发挥出 TiDB 的全部能力，目前 TiDB 同 Flink 与 Presto 对接的读写能力中，仅对读取做了全局事务支持，在写入端尚未支持全局事务。在大数据 ETL 典型的场景下无法保证大事务写入数据的原子可见性，在实时计算场景下也无法利用实时计算的 checkpoint 机制保证全局写入的事务性。因此，我们希望为 TiDB 实现 Flink 和 Presto 的分布式大事务写入能力，充分发挥 TiDB 的能力更好的服务大数据应用场景。

## Detailed Design

- Presto 在 TiDB 作为数据源时，支持分布式事务写入；
- Flink 在 TiDB 作为数据源时，Batch Mode 支持分布式事务写入；
- Flink 在 TiDB 作为数据源时，Streaming Mode 支持按时间窗口或者数据条数分布式事务写入。

## Time-Planning

- 七月：整理/测试/补充 TiKV Java Client 的写入功能；
- 八月：实现 Flink-TiDB-Connector 的分布式事务写入功能；
- 九月：实现 Presto-TiDB-Connector 的分布式事务写入功能；
- 十月：补充集成测试。


## Test Design



### Functional Tests



### Scenario Tests



### Compatibility Tests



### Benchmark Tests



## Impacts & Risks



## Investigation & Alternatives



## Unresolved Questions
