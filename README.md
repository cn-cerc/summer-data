# summer-model

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