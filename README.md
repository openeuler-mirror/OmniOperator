# OmniOperator

# 项目介绍

## 背景

大数据离线计算引擎在互联网、金融、物流等各行各业应用广泛，承担不可或缺的重要角色，而随着业务发展，离线计算引擎的性能逐渐成为瓶颈。以主流的离线计算引擎Spark为例，主要采用Java/Scala等高级编程语言实现，当前对Spark的优化主要是基于Java进行改进，但Java的本身的性能还是弱于Native Code，并且由于Java的语义的限制，对于整体的类SIMD指令支持较弱，无法完全发挥CPU的算力。

## OmniOperator介绍

OmniOperator是鲲鹏自研的大数据SQL计算引擎。OmniOperator实现了高性能算子。充分利用硬件尤其是异构算力的计算能力，使用Native Code实现了Omni算子，相对于原始的Java算子和Scala算子，Omni算子极大地提升了计算引擎的性能。实现了高效数据组织方式。定义了一种与语言无关的列式内存格式，使用堆外内存实现了OmniVec，它可以支持零副本读取数据，且没有序列化开销，使用者能够更高效地处理内存中的数据。

# 版本说明

**当前版本适用于开源软件哪个版本，如**

| 开源软件 | 开源版本            |
| -------- | ------------------- |
| Spark    | 3.3.1、3.4.3、3.5.2 |


# 快速上手

## 源码编译

### 1、依赖组件

- GCC: 10.3.1
- CMAKE: 3.22.0
- JDK: 1.8.0_342
- zlib: 1.2.8
- LLVM: 15.0.4
- googletest: 1.10.0
- jemalloc: 5.2.1
- nlomann json: 3.7.3
- huawei securec: V100R001C01SPC011B003_00001
- autoconf：2.69



### 2、编译命令

OmniOperator编译命令如下

```shell
cd OmniOperatorJIT
bash build_scripts/build.sh release:java --exclude-test

cd bindings/java
mvn clean install -DskipTests
```



## 环境部署

部署请参考以下链接：

https://www.hikunpeng.com/document/detail/zh/kunpengbds/appAccelFeatures/sqlqueryaccelf/kunpengbds_omniruntime_20_0212.html

## 测试验证

* 测试步骤：

1. 使用hive-testbench导入2GB TPCDS数据集
2. 参考下面文档链接，添加omni相关配置文件，并提交sql执行
3. 检查执行计划中的算子是否包含Omni

详细测试验证步骤请参考以下链接：

https://www.hikunpeng.com/document/detail/zh/kunpengbds/appAccelFeatures/sqlqueryaccelf/kunpengbds_omniruntime_20_0241.html





# 贡献指南

如果使用过程中有任何问题，或者需要反馈特性需求和bug报告，可以提交isssues联系我们，具体贡献方法可参考[这里](https://gitcode.com/boostkit/community/blob/master/docs/contributor/contributing.md)。

# 免责声明

此代码仓计划参与Spark软件开源，仅作Spark功能扩展/Spark性能提升，编码风格遵照原生开源软件，继承原生开源软件安全设计，不破坏原生开源软件设计及编码风格和方式，软件的任何漏洞与安全问题，均由相应的上游社区根据其漏洞和安全响应机制解决。请密切关注上游社区发布的通知和版本更新。鲲鹏计算社区对软件的漏洞及安全问题不承担任何责任。

# 许可证书

https://www.apache.org/licenses/LICENSE-2.0

# 参考文档

安装指南：

https://www.hikunpeng.com/document/detail/zh/kunpengbds/appAccelFeatures/sqlqueryaccelf/kunpengbds_omniruntime_20_0212.html

测试验证指南：

https://www.hikunpeng.com/document/detail/zh/kunpengbds/appAccelFeatures/sqlqueryaccelf/kunpengbds_omniruntime_20_0241.html

