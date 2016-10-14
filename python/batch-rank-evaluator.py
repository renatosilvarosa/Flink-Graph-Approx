#! /usr/bin/python3

import glob
import re
import sys
from rank_evaluator import evaluate

lists_dir = sys.argv[1]
exact_pr_dir = sys.argv[2]
p_value = float(sys.argv[3])

pattern = re.compile("\d{4}")

file_names = glob.glob(lists_dir + '/exact-repeat_*')

for name in file_names:
    name2 = name.replace('exact-repeat', 'exact', 1).replace(lists_dir, exact_pr_dir, 1)
    print(pattern.search(name).group(0), ";", evaluate(name, name2, p_value), sep='')
