```shell
cd ./data && git clone https://github.com/ticoAg/chinese-poetry.git
```

# Neo4j import

```shell
docker run -itd \
	--name neo4j \
	-p 7474:7474 \
	-p 7687:7687 \
	--env=neo4j/qwe123456 \
	-v ~/Documents/poem-graph/data/neo4j:/var/lib/neo4j \
	neo4j:5.23.0

# vim conf/neo4j.conf
dbms.default_database=graphdb

# 把数据放在~/Documents/poem-graph/data/neo4j/import下, 清除库并重新导入
neo4j-admin database import full \
    --nodes=./data/neo4j/node.csv \
    --relationships=./data/neo4j/rel.csv \
    --overwrite-destination=true \
    neo4j && \
    neo4j console
```
