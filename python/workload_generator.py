import random
import sys

prob = float(sys.argv[3])

with open(sys.argv[1]) as original:
    with open(sys.argv[2], 'w') as dest:
        for l in original:
            print(l, file=dest, end='')
            if random.random() <= prob:
                print("Q ", file=dest)
