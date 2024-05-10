# summer-data

提供了各类基础性配置、规范与工具。

## 常用工具
ClassConfig：为项目提供配置文件管理，可以嵌套层次调用，支持开发环境与正式环境分离。

ISession：定义运行环境支持对象，管理 token、corpNo 变量，管理 mysql、redis 等外部连接。

Utils：用于处理常见的数字格式化、小数点精度处理、随机数生成、字符串编码等。

## 数据传输

DataSet: 定义内存数据表格式，为sql数据库操作提供基础，可应用于android以及app等项目。

DataRow：定义内存数据行格式，对应数据表中一条记录。

## 时间工具

Datetime、FastDate、FastTime：可处理数据库日期、本地化日期的转换。

## 存储工具

MySQL、MsSQL、MongoDB、Redis、SQLite等用于各类存储库的操作。

# summer-db 模组简介
创建用于java使用的数据表操作工具，其核心为DataSet类，用于建立内存表。

基于DataSet所派生的SqlQuery，为操作各类数据仓库的基类。

具体实现有：

### sql数据库操作

可取代hibernate/mybatis，特别适用各类组合条件查询，支持批处理与事务，也支持与hibernate互转：

* MysqlQuery：用于操作mysql数据表；
* MssqlQuery：用于操作mssql数据表；
* SqliteQuery：用于操作sqlite数据表；

### nosql数据库操作：
* NasQuery：用于以类似mysql的操作方式，操作网络文件或本地文件，降低学习成本，以及由mysql迁移到nas的成本。
* MongoQuery：用于以类似mysql的操作方式，操作MongoDB数据，降低学习成本，以及由mysql迁移到mongo的成本。
* OssQuery：用于以类似mysql的操作方式，操作aliyun-oss数据，降低学习成本，以及由mysql迁移到aliyun-oss的成本。

### 其它对象存储操作：
* RedisRecord：用于以类似mysql的操作方式，操作Redis数据，降低学习成本，以及由mysql迁移到Redis的成本。
* QueueQuery：用于以类似msql的操作方式，操作MNS队列，降低学习成本，以及由mysql迁移到队列的成本。

欢迎大家使用，同时反馈更多的建议与意见，也欢迎其它业内人士，对此项目进行协同改进！

### docker自建环境

RabbitMQ

```shell
docker run -d -p 15672:15672 -p 5672:5672 -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=admin --restart=always --name rabbitmq rabbitmq:management
```

### KnowallLog 使用
```java
log.info("日志内容", KnowallLog.of("data1", "data2", "data3"));
```

```java
KnowallLog log = new KnowallLog(this.getClass(), 10);
log.setLevel("warn");
log.setMessage("测试");
log.addData("data0");
log.post();
```