## 1 环境安装

[安装apache-jena-3.9.0和apache-jena-fuseki-3.9.0](https://archive.apache.org/dist/jena/source/)

## 2 规则导入

将 `rules.ttl` 迁移到`apache-jena-fuseki-3.9.0\run\databases\`

## 3 家谱数据导入

`D:\jena\apache-jena-3.9.0\bat>tdbloader.bat --loc="D:\jena\tdb" "D:\jena\zengguofanjiapu.nt"`

## 4 本地启动Jena推理机

`D:\jena\apache-jena-fuseki-3.9.0>fuseki-server.bat`

## 5 网页启动服务器
`http://localhost:3030/`

## 6 在服务器指令窗口输入以下查询指令，表示查询“曾国藩的孙子”

```
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX : <http://www.zhonghuapu.com#>

SELECT ?names ?intros WHERE {
?f :简介 ?intros.
?d :姓名 '曾国藩'.
?d :孙子 ?f.
?f :姓名 ?names.
}
```
