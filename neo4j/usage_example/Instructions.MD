Copy files to the container using:

```docker cp ./neo4j/usage_example neo4j:./import```

Get into an interactive shell in the neo4j container:

```docker exec -it neo4j sh```

Purge with script:
```
neo4j stop
rm -rf /data/databases/graph.db
```

Standard import:

```neo4j-admin import --nodes=/import/movies.csv --nodes=/import/actors.csv --relationships=/import/roles.csv```

Restart neo4j server:

```neo4j start```
