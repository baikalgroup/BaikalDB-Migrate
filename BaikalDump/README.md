导入导出工具

1. 编译

导出工具dump:  go build -o bin/dump src/dump.go

导入工具load:  go build -o bin/load src/load.go


2. 使用

2.1 dump
   ./bin/dump conf/config.cfg

   配置文件为conf/config.cfg, 填写baikaldb的连接串，及dump路径和格式.


2.2 load
   ./bin/load conf/config.cfg

   需要添加localconfig， destmysql等配置

 
