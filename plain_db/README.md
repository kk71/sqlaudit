PLAIN DB CONNECTORS
===================

老代码使用文本SQL操纵oracle，使用JS直接操纵mongo，为了节省重构时间，老代码暂时不动，所以把老代码连接数据库的工具类拿过来。

在旧代码需要重写的时候，记得尽可能不用 *PLAIN* 的方式操纵数据库。

当然，为了保证某些非常复杂的数据库操作可以使用原生的数据库命令，这块代码还是被保留下来了。