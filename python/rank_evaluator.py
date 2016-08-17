#! /usr/bin/python3

import RBO
import sys


def evaluate(file1, file2, p_value):
    try:
        with open(file1) as f1:
            try:
                with open(file2) as f2:
                    list_to_evaluate = f1.readlines()
                    gold_list = f2.readlines()
                    return RBO.score(list_to_evaluate, gold_list, p=p_value)

            except FileNotFoundError:
                print("File not found:", file2, file=sys.stderr)

    except FileNotFoundError:
        print("File not found:", file1, file=sys.stderr)


if __name__ == "__main__":
    name = sys.argv[1]
    name2 = sys.argv[2]
    p = float(sys.argv[3])
    print(evaluate(name, name2, p))
