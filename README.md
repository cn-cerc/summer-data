# summer-core
包括了 summer-framework 总pom文件，为框架及应用提供 jar 统一版本
提供了各类基础性配置、规范与工具

关键类：
ClassConfig：为项目提供配置文件管理，可以嵌套层次调用，支持开发环境与正式环境分离。
ISession：定义运行环境支持对象，管理 token、corpNo 变量，管理 mysql、redis 等外部连接。
DataSet: 定义内存数据表格式，为sql数据库操作提供基础，可应用于android以及app等项目。
Record：定义内存数据行格式，对应数据表中一条记录。
TDateTime、TDate：对应数据库中的日期时间格式的管理。