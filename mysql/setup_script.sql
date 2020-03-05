CREATE DATABASE IF NOT EXISTS metastore;
USE metastore;
SOURCE /hive-schema-3.1.0.mysql.sql;
CREATE USER 'hive'@'172.21.0.%' IDENTIFIED BY 'root';
GRANT all on *.* to 'hive'@'172.21.0.%';
flush privileges;
