import os
import time
import pickle

from collections import defaultdict
from multiprocessing import Queue, Process

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import SerializationError
from retrying import retry

load_dotenv()


class DBSNP:
    def __init__(self, es_index: str):
        _env = os.environ['ENV']
        self.es = Elasticsearch([f"http://elastic:{os.environ['ES_PASS']}@{os.environ['ES_HOST_' + _env]}:{os.environ['ES_PORT_' + _env]}"], verify_certs=False)
        self.es_index = es_index
        self.last_time = time.time()

    def create_index(self):
        if not self.es.indices.exists(index=self.es_index):
            self.es.indices.create(index=self.es_index, body={
                "settings": {
                    "number_of_replicas": 0,
                    "refresh_interval": -1,
                    "index.max_result_window": 10000000
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
        else:
            print("Using existing index")

    def refresh_index(self):
        self.es.indices.refresh(index=self.es_index)

    def _build_insert_body(self, rsid_and_chrpos: dict):
        body = []
        for rsid, chrpos in rsid_and_chrpos.items():
            body.append(f'{{"index": {{"_id": "{rsid}"}}}}')
            body.append(f'{{"CHR": {chrpos[0]}, "POS": {chrpos[1]}}}')
        return "\n".join(body)

    @retry(wait_fixed=10000)
    def bulk_index(self, rsid_and_chrpos: dict):
        try:
            return self.es.bulk(request_timeout=600, index=self.es_index, body=self._build_insert_body(rsid_and_chrpos))
        except SerializationError as e:
            pass

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
    dbsnp = DBSNP('dbsnp-157')
    dbsnp.create_index()
    dbsnp.timer_lap()

    os.makedirs(wd + '/pickle', exist_ok=True)

    all_new_to_old_rsids = defaultdict(list)
    with open(wd + '/merged.txt', 'r') as f:
        lines = f.readlines()
        print(len(lines), dbsnp.timer_lap())
        for line in lines:
            old_rsid, new_rsid = dbsnp.process_merged_line(line)
            all_new_to_old_rsids[new_rsid].append(old_rsid)

    print(len(all_new_to_old_rsids), dbsnp.timer_lap(), "\n")

    queue = Queue()
    n_consumers = 16

    def producer(queue, n_consumers):
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
                    batch_number = count // batch_size
                    with open(wd + f'/pickle/{batch_number}', 'wb') as f:
                        pickle.dump(rsid_and_chrpos, f)
                    queue.put(batch_number)
                    rsid_and_chrpos = {}
                    print(f"Batch {batch_number} added for line ending {count} with accumulated merger {count_old} in {dbsnp.timer_lap()}")
        for _ in range(n_consumers):
            queue.put(None)

    def consumer(consumer_id, queue):
        while True:
            batch_number = queue.get()
            if batch_number is None:
                break
            t = time.time()
            with open(wd + f'/pickle/{batch_number}', 'rb') as f:
                rsid_and_chrpos = pickle.load(f)
            print(f"Starting batch {batch_number}")
            dbsnp.bulk_index(rsid_and_chrpos)
            print(f"Batch {batch_number} processed by consumer {consumer_id} with length {len(rsid_and_chrpos)} in {round(time.time() - t, 4)}")

    producer_process = Process(target=producer, args=(queue, n_consumers))
    producer_process.start()

    consumer_processes = []
    for i in range(n_consumers):
        cp = Process(target=consumer, args=(i + 1, queue))
        cp.start()
        consumer_processes.append(cp)

    for cp in consumer_processes:
        cp.join()
