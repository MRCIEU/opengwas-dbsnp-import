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


#
ALTER INSTANCE DISABLE INNODB REDO_LOG;

#
show variables like '%max_connections%';
set global max_connections = 500;

show status like  'Threads%';

# Check number of records in total in dbsnp
SELECT
    sum(TABLE_ROWS)
FROM
    information_schema.PARTITIONS
WHERE
    TABLE_SCHEMA = 'opengwas'
    AND TABLE_NAME = 'dbsnp';


# Check number of records by partitions in dbsnp
SELECT
    PARTITION_NAME,
    TABLE_ROWS
FROM
    information_schema.PARTITIONS
WHERE
    TABLE_SCHEMA = 'opengwas'
    AND TABLE_NAME = 'dbsnp';