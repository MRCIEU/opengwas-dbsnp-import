import os
import redis
import subprocess
import time

from dotenv import load_dotenv

load_dotenv()


class DBSNP:
    def __init__(self, redis_key: str):
        self.r = redis.Redis(host=os.environ['REDIS_HOST'], port=os.environ['REDIS_PORT'], password=os.environ['REDIS_PASS'], db=os.environ['REDIS_DB'], decode_responses=True)
        self.redis_key = redis_key
        self.last_time = time.time()

    def import_batch(self, rsid_and_chrpos: dict) -> int:
        if len(rsid_and_chrpos) == 0:
            return 0
        self.r.hset(self.redis_key, mapping=rsid_and_chrpos)
        return self.r.hlen(self.redis_key)

    def get_batch(self, rsids: list) -> dict:
        return self.r.hmget(self.redis_key, rsids)

    def zip_rsid_with_chrpos(self, rsids: list, chrpos: list) -> dict:
        return {rsid: chrpos for rsid, chrpos in zip(rsids, chrpos) if chrpos is not None}

    def process_chr_pos_rsid_line(self, line: str) -> tuple:
        parts = line.rstrip('\n').split()
        return int(parts[2][2:]), f"{parts[0]}:{parts[1]}"

    def process_merged_line(self, line: str) -> tuple:
        parts = line.rstrip('\n').split()
        return parts[0], parts[1]

    def timer_lap(self):
        current_time = time.time()
        elapsed_time = current_time - self.last_time
        self.last_time = current_time
        return elapsed_time


def _count_lines(filename: str) -> int:
    return int(subprocess.getoutput("wc -l %s" % filename).split()[0])


if __name__ == '__main__':
    wd = os.environ['WORKING_DIR']
    dbsnp = DBSNP('dbsnp_157')

    t = time.time()

    count = 0
    batch_size = 1_000_000
    rsid_and_chrpos = {}
    n_lines = _count_lines(wd + '/chr_pos_rsid.txt')
    with open(wd + '/chr_pos_rsid.txt', 'r') as f:
        for line in f:
            rsid_without_rs, chrpos = dbsnp.process_chr_pos_rsid_line(line)
            rsid_and_chrpos[rsid_without_rs] = chrpos
            count += 1
            if count % batch_size == 0 or count == n_lines:
                print(count // batch_size, dbsnp.timer_lap())
                dbsnp.import_batch(rsid_and_chrpos)
                rsid_and_chrpos = {}
                print('batch ending at line', count, dbsnp.timer_lap())

    count = 0
    batch_size = 100_000
    old_rsids = []
    new_rsids = []
    rsid_and_chrpos = {}
    n_lines = _count_lines(wd + '/merged.txt')
    with open(wd + '/merged.txt', 'r') as f:
        for line in f:
            old_rsid, new_rsid = dbsnp.process_merged_line(line)
            old_rsids.append(old_rsid)
            new_rsids.append(new_rsid)
            count += 1
            if count % batch_size == 0 or count == n_lines:
                print(count // batch_size, dbsnp.timer_lap())
                for i, chrpos in enumerate(dbsnp.get_batch(new_rsids)):
                    if chrpos is not None:
                        rsid_and_chrpos[old_rsids[i]] = chrpos
                print(len(rsid_and_chrpos), dbsnp.timer_lap())
                for i, chrpos in enumerate(dbsnp.get_batch(old_rsids)):
                    if chrpos is not None:
                        print(old_rsids[i])
                dbsnp.import_batch(rsid_and_chrpos)
                old_rsids = []
                new_rsids = []
                rsid_and_chrpos = {}
                print('batch ending at line', count, dbsnp.timer_lap())
