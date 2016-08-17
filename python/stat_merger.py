#! /usr/bin/python3

import sys
from glob import glob
from pathlib import Path

# stats_dir = "/home/rrosa/Statistics/CitHepPh/PR"
stats_dir = "D:\Documents\Dissertação\Statistics\CitHepPh\PR"
# eval_dir = "/home/rrosa/Eval"
eval_dir = "D:\Documents\Dissertação\Eval\\"
iterations = 30
base_threshold = 0.0
top_threshold = 0.25
step_threshold = 0.05
base_neigh = 0
top_neigh = 3
output_size = 1000
result_prefix = "PR-CitHepPh"

p = Path(stats_dir)
p.glob("*.csv")
file_names = glob(stats_dir + '/*.csv')

lines_dict = dict()

for file in p.glob("*.csv"):
    with file.open() as s_file:
        lines = s_file.readlines()
        header = lines[0].strip()
        for l in lines[1:]:
            lines_dict[int(l.split(";")[0])] = l.strip()

    try:
        with open(eval_dir + file.name) as e_file:
            for l in e_file:
                (n, ev) = l.split(";")
                it = int(n)
                if it in lines_dict:
                    ln = lines_dict[it]
                    lines_dict[it] = ln + ";" + ev.rstrip().replace(".", ",")
    except FileNotFoundError:
        print("File not found:", eval_dir + file.name, file=sys.stderr)
        continue

    with file.open("w") as s_file:
        header += ";eval"
        print(header, file=s_file)
        for k in sorted(lines_dict.keys()):
            print(lines_dict[k], file=s_file)
