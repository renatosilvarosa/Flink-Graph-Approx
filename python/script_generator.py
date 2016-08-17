#! /usr/bin/python3

main_class = "test.hits.cithepph.HITSCitHepPhApprox"
local_dir = "/home/rrosa"
remote_dir = "/home/rrosa"
python_dir = remote_dir + "/Flink-Graph-Approx/python"
iterations = 30
base_threshold = 0.0
top_threshold = 0.25
step_threshold = 0.05
base_neigh = 0
top_neigh = 3
output_size = 1000
result_prefix = "CitHepPh"

th = base_threshold
while th <= top_threshold:
    neigh = base_neigh
    while neigh <= top_neigh:
        th = round(th, 2)
        print("java -cp .:flink-graph-tester-0.3.jar:dependency/* {0} {1} {2} {3} {4:0.2f} {5} {6} {7}-{4:0.2f}-{5}.csv"
              .format(main_class, local_dir, remote_dir, iterations, th, neigh, output_size, result_prefix))
        print(python_dir + "/batch-rank-evaluator.py {0}/Results/HITS/{1}-{2:0.2f}-{3} {0}/Results/HITS/{1}-exact 0.99 "
                           "> {0}/Eval/HITS/{1}-{2:0.2f}-{3}.csv".format(remote_dir, result_prefix, th, neigh))
        print('')
        neigh += 1
    th += step_threshold
