#! /usr/bin/python3

import sys
from pathlib import Path

eval_dir = sys.argv[1]

iterations = 30
base_threshold = 0.0
top_threshold = 0.25
step_threshold = 0.05
base_neigh = 0
top_neigh = 3
output_size = 1000

p = Path(eval_dir)
lines_dict = dict()
evals_dict = dict()
evals_header = list()

for file in p.glob("*.csv"):
    try:
        with file.open() as e_file:
            evals_header.append(file.name)
            for l in e_file:
                (n, ev) = l.split(";")
                it = int(n)
                evals_dict.setdefault(it, list()).append(ev.rstrip().replace(".", ","))
    except FileNotFoundError:
        print("File not found:", eval_dir + file.name, file=sys.stderr)
        continue

print(";".join(evals_header))
for it in sorted(evals_dict):
    print(";".join(evals_dict[it]))
