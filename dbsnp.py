import os
import time

from collections import defaultdict

from elasticsearch import Elasticsearch
from dotenv import load_dotenv

load_dotenv()


class DBSNP:
    def __init__(self, es_index: str):
        _env = os.environ['ENV']
        self.es = Elasticsearch([f"http://elastic:{os.environ['ES_PASS']}@{os.environ['ES_HOST_' + _env]}:{os.environ['ES_PORT_' + _env]}"], verify_certs=False)
        self.es_index = es_index
        self.last_time = time.time()

    def create_index(self):
        self.es.indices.create(index=self.es_index, body={
            "settings": {
                "number_of_replicas": 0,
                "refresh_interval": -1
            },
            "mappings": {
                "properties": {
                    "CHR": {
                        "type": "keyword"
                    },
                    "POS": {
                        "type": "long"
                    }
                }
            }
        })

    def refresh_index(self):
        self.es.indices.refresh(index=self.es_index)

    def _build_insert_body(self, rsid_and_chrpos: dict):
        body = []
        for rsid, chrpos in rsid_and_chrpos.items():
            body.append(f'{{"index": {{"_id": "{rsid}"}}}}')
            body.append(f'{{"CHR": {chrpos[0]}, "POS": {chrpos[1]}}}')
        return "\n".join(body)

    def bulk_index(self, rsid_and_chrpos: dict) -> dict:
        return self.es.bulk(request_timeout=600, index=self.es_index, body=self._build_insert_body(rsid_and_chrpos))

    def _build_mget_body(self, rsids: list):
        return {"docs": [{'_id': rsid} for rsid in rsids]}

    def mget(self, rsids: list) -> dict:
        response = self.es.mget(request_timeout=600, index=self.es_index, body=self._build_mget_body(rsids))['docs']
        result = {}
        for doc in response:
            if doc['found']:
                result[doc['_id']] = (doc['_source']['CHR'], doc['_source']['POS'])
        return result

    def process_chr_pos_rsid_line(self, line: str) -> tuple:
        parts = line.rstrip('\n').split()
        return parts[2], (parts[0], parts[1])

    def process_merged_line(self, line: str) -> tuple:
        parts = line.rstrip('\n').split()
        return f"rs{parts[0]}", f"rs{parts[1]}"

    def timer_lap(self):
        current_time = time.time()
        elapsed_time = current_time - self.last_time
        self.last_time = current_time
        return round(elapsed_time, 4)


if __name__ == '__main__':
    wd = os.environ['WORKING_DIR']
    dbsnp = DBSNP('dbsnp-157-2')
    dbsnp.create_index()
    dbsnp.timer_lap()

    all_new_to_old_rsids = defaultdict(list)
    with open(wd + '/merged.txt', 'r') as f:
        lines = f.readlines()
        print(len(lines), dbsnp.timer_lap())
        for line in lines:
            old_rsid, new_rsid = dbsnp.process_merged_line(line)
            all_new_to_old_rsids[new_rsid].append(old_rsid)

    print(len(all_new_to_old_rsids), dbsnp.timer_lap(), "\n")

    count = 0
    count_old = 0
    batch_size = 1_000_000
    rsid_and_chrpos = {}
    n_lines = 1145968637
    with open(wd + '/chr_pos_rsid.txt', 'r') as f:
        for line in f:
            rsid, chr_pos_tuple = dbsnp.process_chr_pos_rsid_line(line)
            rsid_and_chrpos[rsid] = chr_pos_tuple
            if rsid in all_new_to_old_rsids:
                for old_rsid in all_new_to_old_rsids[rsid]:
                    rsid_and_chrpos[old_rsid] = chr_pos_tuple
                    count_old += 1
            count += 1
            if count % batch_size == 0 or count == n_lines:
                print(count // batch_size, dbsnp.timer_lap())
                dbsnp.bulk_index(rsid_and_chrpos)
                rsid_and_chrpos = {}
                print('batch ending at line', count, count_old, dbsnp.timer_lap(), "\n")

    # dbsnp.refresh_index()
    # dbsnp.timer_lap()

    # count = 0
    # batch_size = 10_000
    # old_to_new_rsids = {}
    # new_rsids = []
    # rsid_and_chrpos = {}
    # n_lines = 11963907
    # with open(wd + '/merged.txt', 'r') as f:
    #     for line in f:
    #         old_rsid, new_rsid = dbsnp.process_merged_line(line)
    #         old_to_new_rsids[old_rsid] = new_rsid
    #         new_rsids.append(new_rsid)
    #         count += 1
    #         if count % batch_size == 0 or count == n_lines:
    #             print(count // batch_size, dbsnp.timer_lap())
    #             chr_pos_of_new_rsids = dbsnp.mget(new_rsids)
    #             print(len(chr_pos_of_new_rsids), dbsnp.timer_lap())
    #             for old_rsid in old_to_new_rsids.keys():
    #                 new_rsid = old_to_new_rsids[old_rsid]
    #                 if new_rsid in chr_pos_of_new_rsids:
    #                     rsid_and_chrpos[old_rsid] = chr_pos_of_new_rsids[new_rsid]
    #             print(len(rsid_and_chrpos), dbsnp.timer_lap())
    #             dbsnp.bulk_index(rsid_and_chrpos)
    #             old_to_new_rsids = {}
    #             new_rsids = []
    #             rsid_and_chrpos = {}
    #             print('batch ending at line', count, dbsnp.timer_lap(), "\n")
