sqlaudit
========

### 简介

全部配置都在settings.py文件中，并且每个配置都给了默认值，常规情况下默认值不需要修改。
如果确实需要修改，请在运行环境中输出配置的临时值（若在docker-compose运行，请修改py.env）

例如：测试api，需要先打开api测试页，而api测试页默认是关闭状态。这时需要在py.env里输出API_DOC=1，启动后进入/apidoc即可


### 架构概述

任何层都可以向上（包括跳级）引用，但是不能向下一层引用。
除了Core，其他三层都可实现restful API。

* Core：核心抽象层，不涉及具体代码，不引用第三方包，不引用目录以外的代码，只实现抽象基类。

* Component：rule, auth, cmdb, ticket, task, etc.
基础组件层，但是与任何具体纳管库无关。同层的组件之间允许耦合，但尽量减低。

* CMDB：oracle_cmdb, mysql_cmdb, etc.
纳管库层，实现各自支持的纳管库类型对应的代码，同层组件之间不允许耦合。

* Coupling：聚合层，对于业务需要的耦合，在本层实现。
