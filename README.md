### opengwas-dbsnp-import
Build the rsid -> chr:pos mapping for OpenGWAS using dbSNP data

### Prerequisites

https://ftp.ncbi.nih.gov/snp/latest_release/VCF/ (GRCh37)
- GCF_000001405.25.gz
- GCF_000001405.25.gz.tbi

https://ftp.ncbi.nih.gov/snp/organisms/human_9606/database/organism_data/
- RsMergeArch.bcp.gz

### Build contig to chromosome mapping
Generate `GCF_000001405.25.gz.csi`
```
bcftools index -c GCF_000001405.25.gz
```

Check number of records under each chromosome
```
bcftools index -s GCF_000001405.25.gz > summary.txt
```

```
> head summary.txt
NC_000001.10	.	89641618
NC_000002.11	.	96370672
NC_000003.11	.	78832062
NC_000004.11	.	76396408
NC_000005.9	.	71360093
NC_000006.11	.	66749505
NC_000007.13	.	63658553
NC_000008.10	.	60127211
NC_000009.11	.	49510218
NC_000010.10	.	52814699
```

Build the mapping `chr_map.txt` (manually and only keeping chr 1-22, X and Y)

```
NC_000001.10	1
NC_000002.11	2
NC_000003.11	3
NC_000004.11	4
NC_000005.9	5
NC_000006.11	6
NC_000007.13	7
NC_000008.10	8
NC_000009.11	9
NC_000010.10	10
NC_000011.9	11
NC_000012.11	12
NC_000013.10	13
NC_000014.8	14
NC_000015.9	15
NC_000016.9	16
NC_000017.10	17
NC_000018.9	18
NC_000019.9	19
NC_000020.10	20
NC_000021.8	21
NC_000022.10	22
NC_000023.10	X
NC_000024.9	Y
```

### Extract chr, pos and rsid columns

Extract contig, pos and rsid columns from the VCF file
```
bcftools query -f'%CHROM %POS %ID\n' GCF_000001405.25.gz > contig_pos_rsid.txt
```

```
> head contig_pos_rsid.txt
NC_000001.10 10001 rs1570391677
NC_000001.10 10002 rs1570391692
NC_000001.10 10003 rs1570391694
NC_000001.10 10007 rs1639538116
NC_000001.10 10008 rs1570391698
NC_000001.10 10009 rs1570391702
NC_000001.10 10013 rs1639538192
NC_000001.10 10013 rs1639538231
NC_000001.10 10014 rs1639538207
NC_000001.10 10015 rs1570391706
```

Replace contig with chr

```
awk 'NR==FNR{a[$1]=$2; next} $1 in a {print a[$1], $2, $3}' chr_map.txt contig_pos_rsid.txt > chr_pos_rsid.txt
```
```
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

Get mapping of merged rsids

```
gunzip -c RsMergeArch.bcp.gz | awk '{print $1, $2}' > merged.txt
```
```
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

e.g. rs34183431 has been merged to rs10710027, so when the users give rs34183431 they should be redirected to rs10710027


