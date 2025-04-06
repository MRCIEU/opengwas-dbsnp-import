import os
import redis
import time

from dotenv import load_dotenv

load_dotenv()


class DBSNP:
    def __init__(self, redis_key: str):
        self.r = redis.Redis(host=os.environ['REDIS_HOST'], port=os.environ['REDIS_PORT'], password=os.environ['REDIS_PASS'], db=os.environ['REDIS_DB'])
        self.redis_key = redis_key
        self.last_time = time.time()

    def import_batch(self, rsid_and_chrpos: dict) -> None:
        self.r.hset(self.redis_key, mapping=rsid_and_chrpos)
        return self.r.hlen(self.redis_key)

    def get_batch(self, rsids: list) -> dict:
        return self.r.hmget(self.redis_key, rsids)

    def zip_rsid_with_chrpos(self, rsids: list, chrpos: list) -> dict:
        return {rsid: chrpos for rsid, chrpos in zip(rsids, chrpos) if chrpos is not None}

    def process_chr_pos_rsid_line(self, line: str) -> tuple:
        parts = line.rstrip('\n').split()
        return int(parts[2][2:]), f"{parts[0]}:{parts[1]}"

    def timer_lap(self):
        current_time = time.time()
        elapsed_time = current_time - self.last_time
        self.last_time = current_time
        return elapsed_time


if __name__ == '__main__':
    wd = os.environ['WORKING_DIR']
    dbsnp = DBSNP('dbsnp_157')
    count = 0
    batch_size = 1_000_000
    rsid_and_chrpos = {}
    t = time.time()
    with open(wd + '/chr_pos_rsid.txt', 'r') as f:
        for line in f:
            rsid_without_rs, chrpos = dbsnp.process_chr_pos_rsid_line(line)
            rsid_and_chrpos[rsid_without_rs] = chrpos
            count += 1
            if count % batch_size == 0:
                print(count // batch_size, dbsnp.timer_lap())
                dbsnp.import_batch(rsid_and_chrpos)
                rsid_and_chrpos = {}
                print(dbsnp.timer_lap())
