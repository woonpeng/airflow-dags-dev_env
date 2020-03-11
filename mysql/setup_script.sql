CREATE DATABASE IF NOT EXISTS metastore;
USE metastore;
SOURCE /hive-schema-3.1.0.mysql.sql;
CREATE USER 'hive'@'%' IDENTIFIED BY 'root';
GRANT all on *.* to 'hive'@'%';
flush privileges;

ALTER USER 'hive'@'%' IDENTIFIED WITH mysql_native_password BY 'root';
