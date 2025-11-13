## opengwas-dbsnp-import
Build the rsid -> chr:pos mapping for OpenGWAS using dbSNP data

### Prerequisites

https://ftp.ncbi.nih.gov/snp/latest_release/VCF/ (GRCh38)
- GCF_000001405.40.gz
- GCF_000001405.40.gz.tbi

https://ftp.ncbi.nih.gov/snp/organisms/human_9606/database/organism_data/
- RsMergeArch.bcp.gz

### Build contig to chromosome mapping
Generate `GCF_000001405.40.gz.csi`
```shell
bcftools index -c GCF_000001405.40.gz
```

Check number of records under each chromosome
```shell
bcftools index -s GCF_000001405.40.gz > summary.txt
```

```shell
> head summary.txt
NC_000001.11	.	93124941
NC_000002.12	.	98008402
NC_000003.12	.	80069959
NC_000004.12	.	77041552
NC_000005.10	.	72149175
NC_000006.12	.	67611803
NC_000007.14	.	64876702
NC_000008.11	.	60505409
NC_000009.12	.	50922544
NC_000010.11	.	54197320
```

Build the mapping `chr_map.txt` (only keeping chr 1-22, X and Y)

```shell
grep -E '^NC_0000[0-9]+\.[0-9]+' summary.txt \
  | sort -k1,1 -n \
  | awk -F'\t' '{
    chr=$1
    sub(/^NC_0*/, "", chr)
    sub(/\..*$/, "", chr)
    if (chr == "23") chr = "X"
    else if (chr == "24") chr = "Y"
    print $1 "\t" chr
  }' > chr_map.txt
```

```
NC_000001.11	1
NC_000002.12	2
NC_000003.12	3
NC_000004.12	4
NC_000005.10	5
NC_000006.12	6
NC_000007.14	7
NC_000008.11	8
NC_000009.12	9
NC_000010.11	10
NC_000011.10	11
NC_000012.12	12
NC_000013.11	13
NC_000014.9	14
NC_000015.10	15
NC_000016.10	16
NC_000017.11	17
NC_000018.10	18
NC_000019.10	19
NC_000020.11	20
NC_000021.9	21
NC_000022.11	22
NC_000023.11	X
NC_000024.10	Y
```

### Extract chr, pos and rsid columns

Extract contig, pos and rsid columns from the VCF file
```shell
bcftools query -f'%CHROM %POS %ID\n' GCF_000001405.40.gz > contig_pos_rsid.txt
```

```shell
> head contig_pos_rsid.txt
NC_000001.11 10001 rs1570391677
NC_000001.11 10002 rs1570391692
NC_000001.11 10003 rs1570391694
NC_000001.11 10007 rs1639538116
NC_000001.11 10008 rs1570391698
NC_000001.11 10009 rs1570391702
NC_000001.11 10013 rs1639538192
NC_000001.11 10013 rs1639538231
NC_000001.11 10014 rs1639538207
NC_000001.11 10015 rs1570391706
```

Replace contig with chr

```shell
awk 'NR==FNR{a[$1]=$2; next} $1 in a {print a[$1], $2, $3}' chr_map.txt contig_pos_rsid.txt > chr_pos_rsid.txt
```

```shell
> head chr_pos_rsid.txt
1 10001 rs1570391677
1 10002 rs1570391692
1 10003 rs1570391694
1 10007 rs1639538116
1 10008 rs1570391698
1 10009 rs1570391702
1 10013 rs1639538192
1 10013 rs1639538231
1 10014 rs1639538207
1 10015 rs1570391706
```

```shell
> cat chr_pos_rsid.txt | wc -l
1172651987
```

Get mapping of merged rsids

```shell
gunzip -c RsMergeArch.bcp.gz | awk '{print $1, $2}' > merged.txt
```

```shell
> head merged.txt
34183431 10710027
36106465 35937617
71369126 59193406
144735719 112348376
145510384 3070005
151101600 35181795
200407829 57274482
370530057 57201464
397690673 34500058
397696453 60788024
```

e.g. rs34183431 has been merged to rs10710027, so when the users give rs34183431 (alias) they should be redirected to rs10710027 (original)

### Run the script

```shell
python dbsnp.py /path/to/dbsnp_dir [batch_size]
```

`/path/to/dbsnp_dir` is where you put `chr_pos_rsid.txt` and `merged.txt`. `[batch_size]` is optional, default to 10000000.

This will generate `dbsnp.csv` in less than 30 minutes:

```shell
> head dbsnp.csv
1,1,10001,1570391677
2,1,10002,1570391692
3,1,10003,1570391694
4,1,10007,1639538116
5,1,10008,1570391698
6,1,10009,1570391702
7,1,10013,1639538192
8,1,10013,1639538231
9,1,10014,1639538207
10,1,10015,1570391706
```

Upload this file to MySQL server under `/var/lib/mysql-files/`. Open a `tmux` session, connect to the instance. Create schema and table using `dbsnp.sql`, then

```sql
LOAD DATA INFILE '/var/lib/mysql-files/dbsnp.csv' into table dbsnp FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (chr_id, pos, rsid);
```

```
Query OK, 1182579974 rows affected (2 hours 6 min 11.61 sec)
Records: 1182579974  Deleted: 0  Skipped: 0  Warnings: 0
```

Then create index:

```sql
CREATE INDEX dbsnp_rsid_index ON dbsnp(rsid);
```

```
Query OK, 0 rows affected (6 hours 4 min 57.86 sec)
Records: 0  Duplicates: 0  Warnings: 0
```

`ls -lSh | grep dnsnp` can be used to check for progress.

### Notes

- Why don't just use `primary key (rsid)`?
  - Some rsid may map to multiple locations. E.g. in b37, rs2267 will be at 23:1935021 and 24:1885021
- Why don't just use `primary key (id)`?
  - "Every unique key on the table must use every column in the table's partitioning expression". https://dev.mysql.com/doc/refman/8.0/en/partitioning-limitations-partitioning-keys-unique-keys.html
- Why don't use `primary key (rsid, id)`?
  - "To use the AUTO_INCREMENT mechanism with an InnoDB table, an AUTO_INCREMENT column must be defined as the first or only column of some index ..." https://dev.mysql.com/doc/refman/8.0/en/innodb-auto-increment-handling.html
