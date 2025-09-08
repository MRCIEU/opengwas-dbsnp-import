#!/usr/bin/env python3
import os
import sys
import time

def main():
    if len(sys.argv) < 2:
        print("Usage: python merge_rsid.py /path/to/dbsnp_dir [batch_size]")
        sys.exit(1)

    wd = sys.argv[1]
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 10_000_000

    os.chdir(wd)

    output_file = "dbsnp.csv"
    if os.path.exists(output_file):
        os.remove(output_file)

    merged_file = "merged.txt"
    main_file = "chr_pos_rsid.txt"

    # --- Step 1: Load merged.txt ---
    print("Loading merged.txt ...")
    start_load = time.time()
    all_original_to_alias_rsids = {}
    with open(merged_file) as f:
        for line in f:
            alias, original = line.strip().split()
            all_original_to_alias_rsids.setdefault(original, []).append(alias)
    end_load = time.time()
    print(f"original_to_alias generated in {end_load - start_load:.2f} seconds")

    # --- Step 2: Process main file ---
    seq = 0
    line_count = 0
    batch_count = 0
    batch_start_time = time.time()

    with open(main_file) as infile, open(output_file, "w") as outfile:
        for line in infile:
            chr_str, pos, rsid_str = line.strip().split()
            rsid_num = rsid_str[2:]

            # chr mapping
            chr_num = {'X': 23, 'Y': 24, 'MT': 25}.get(chr_str, chr_str)

            # original rsid
            seq += 1
            outfile.write(f"{seq},{chr_num},{pos},{rsid_num}\n")

            # alias rsids if exist
            for alias in all_original_to_alias_rsids.get(rsid_num, []):
                seq += 1
                outfile.write(f"{seq},{chr_num},{pos},{alias}\n")

            line_count += 1
            if line_count % batch_size == 0:
                batch_count += 1
                now = time.time()
                elapsed = now - batch_start_time
                print(f"Processed batch {batch_count}, elapsed {elapsed:.1f} seconds")
                batch_start_time = time.time()

    # Check for final batch
    if line_count % batch_size != 0:
        batch_count += 1
        elapsed = time.time() - batch_start_time
        print(f"Processed batch {batch_count}, elapsed {elapsed:.1f} seconds")

    total_elapsed = time.time() - start_load
    print(f"All done. Total elapsed time: {total_elapsed:.1f} seconds")


if __name__ == "__main__":
    main()
