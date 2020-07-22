# Setting up Test Client for Neo4j Server

Run `setup-test-client.sh`.

Test the commands:

`ssh root@neo4j manager.sh purge`

will purge the existing database

`ssh root@neo4j manager.sh backup`

will backup the existing database

`ssh root@neo4j manager.sh restore`

will restore the last backup

`ssh root@neo4j`

should fail as the server is set up to only accept the extension commands
