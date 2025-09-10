create schema opengwas;

create table if not exists opengwas.dbsnp
(
    id     bigint unsigned auto_increment,
    chr_id tinyint unsigned not null,
    pos    int unsigned     not null,
    rsid   bigint           not null,
    primary key (id, rsid)
)
    comment 'build 157 GRCh38'
    partition by hash ( rsid ) partitions 32
;

-- Disable binary logging and InnoDB redo log for faster bulk loading
ALTER INSTANCE DISABLE INNODB REDO_LOG;

SET autocommit = 0;
SET foreign_key_checks = 0;
SET unique_checks = 0;
SET GLOBAL innodb_flush_log_at_trx_commit = 2;
SET GLOBAL sync_binlog = 0;
SET sql_log_bin = 0;

-- Load data from CSV file into dbsnp table
LOAD DATA INFILE '/var/lib/mysql-files/dbsnp.csv' into table dbsnp FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

-- Create index on rsid
CREATE INDEX dbsnp_rsid_index ON dbsnp(rsid);

-- Check number of records in total in dbsnp
SELECT
    sum(TABLE_ROWS)
FROM
    information_schema.PARTITIONS
WHERE
    TABLE_SCHEMA = 'opengwas'
    AND TABLE_NAME = 'dbsnp';

-- Check number of records by partitions in dbsnp
SELECT
    PARTITION_NAME,
    TABLE_ROWS
FROM
    information_schema.PARTITIONS
WHERE
    TABLE_SCHEMA = 'opengwas'
    AND TABLE_NAME = 'dbsnp';

-- Re-enable binary logging and InnoDB redo log
ALTER INSTANCE ENABLE INNODB REDO_LOG;

SET autocommit = 1;
SET foreign_key_checks = 1;
SET unique_checks = 1;
SET GLOBAL innodb_flush_log_at_trx_commit = 1;
SET GLOBAL sync_binlog = 1;
SET sql_log_bin = 1;
