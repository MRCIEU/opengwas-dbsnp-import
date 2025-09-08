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

### Run the script (Elasticsearch)


Set up network
```shell
docker network create opengwas-api_og-dbsnp
```

Set up (Elasticsearch)
```shell
docker run -d \
  --name og-dbsnp-es \
  -p 19200:9200 \
  -p 19300:9300 \
  --network opengwas-api_og-dbsnp \
  -e "discovery.type=single-node" \
  -e "path.repo=/mnt/repo" \
  -v /data/opengwas-dbsnp-import/elasticsearch/data:/usr/share/elasticsearch/data \
  -v /data/opengwas-dbsnp-import/elasticsearch/logs:/usr/share/elasticsearch/logs \
  -v /data/opengwas-dbsnp-import/elasticsearch/repo:/mnt/repo \
  elasticsearch:7.13.4
  
docker run -d \
  --name og-dbsnp-es-kibana \
  -p 15601:5601 \
  --network opengwas-api_og-dbsnp \
  -e ELASTICSEARCH_HOSTS=http://og-dbsnp-es:9200 \
  kibana:7.13.4
```

Set up (MySQL)
```shell
docker run -d \
 --name og-dbsnp-mysql \
 --network opengwas-api_og-dbsnp \
 --env-file .env \
 -p 13306:3306 \
 -v /data/opengwas-dbsnp-import/mysql:/var/lib/mysql \
 mysql:8.0.42
```

Run the script
```shell
docker build -t opengwas-dbsnp-import .

docker run -d \
  --name og-dbsnp-import \
  --network opengwas-api_og-dbsnp \
  -v /data/opengwas-dbsnp-import:/opengwas-dbsnp-import \
  opengwas-dbsnp-import

tmux

docker exec -it og-dbsnp-import /bin/sh

cd opengwas-dbsnp-import

python -u dbsnp.py 2>&1 | tee -a dbsnp.log
```


Results (Elasticsearch)

```shell
> cat chr_pos_rsid.txt | wc -l
1172651987
> cat merged.txt | wc -l
11963907
```

```http request
GET dbsnp-157/_flush

GET dbsnp-157/_refresh

GET dbsnp-157/_count
```
```
{
  "count" : 1105824027,
  "_shards" : {
    "total" : 1,
    "successful" : 1,
    "skipped" : 0,
    "failed" : 0
  }
}
```

### Create snapshot and transfer

Create repository
```http request
PUT _snapshot/backup
{
  "type": "fs",
  "settings": {
    "location": "/mnt/repo"
  },
  "compress": true
}
```

Create snapshot
```http request
PUT _snapshot/backup/dbsnp-157
{
  "indices": ["dbsnp-157"],
  "include_global_state": false
}

GET _snapshot/_status
```

Restore on target cluster
```http request
POST _snapshot/backup/dbsnp-157/_restore
{
  "indices": "dbsnp-157"
}
```