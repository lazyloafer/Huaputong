## 1 环境安装

[安装apache-jena-3.9.0和apache-jena-fuseki-3.9.0](https://archive.apache.org/dist/jena/source/)

## 2 规则导入

将 `rules.ttl` 迁移到`apache-jena-fuseki-3.9.0\run\databases\`

## 3 家谱数据导入

`D:\jena\apache-jena-3.9.0\bat>tdbloader.bat --loc="D:\jena\tdb" "D:\jena\zengguofanjiapu.nt"`

## 4 启动Jena推理机

`D:\jena\apache-jena-fuseki-3.9.0>fuseki-server.bat`
