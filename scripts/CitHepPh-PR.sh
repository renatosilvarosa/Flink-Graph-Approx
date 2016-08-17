#!/bin/bash

mkdir -p /home/rrosa/Eval/PR
java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhExact /home/rrosa /home/rrosa 30 1000 CitHepPh-exact.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.00 0 1000 CitHepPh-0.00-0.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.00-0 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.00-0.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.00 1 1000 CitHepPh-0.00-1.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.00-1 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.00-1.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.00 2 1000 CitHepPh-0.00-2.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.00-2 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.00-2.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.00 3 1000 CitHepPh-0.00-3.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.00-3 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.00-3.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.05 0 1000 CitHepPh-0.05-0.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.05-0 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.05-0.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.05 1 1000 CitHepPh-0.05-1.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.05-1 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.05-1.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.05 2 1000 CitHepPh-0.05-2.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.05-2 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.05-2.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.05 3 1000 CitHepPh-0.05-3.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.05-3 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.05-3.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.10 0 1000 CitHepPh-0.10-0.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.10-0 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.10-0.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.10 1 1000 CitHepPh-0.10-1.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.10-1 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.10-1.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.10 2 1000 CitHepPh-0.10-2.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.10-2 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.10-2.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.10 3 1000 CitHepPh-0.10-3.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.10-3 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.10-3.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.15 0 1000 CitHepPh-0.15-0.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.15-0 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.15-0.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.15 1 1000 CitHepPh-0.15-1.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.15-1 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.15-1.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.15 2 1000 CitHepPh-0.15-2.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.15-2 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.15-2.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.15 3 1000 CitHepPh-0.15-3.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.15-3 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.15-3.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.20 0 1000 CitHepPh-0.20-0.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.20-0 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.20-0.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.20 1 1000 CitHepPh-0.20-1.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.20-1 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.20-1.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.20 2 1000 CitHepPh-0.20-2.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.20-2 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.20-2.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.20 3 1000 CitHepPh-0.20-3.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.20-3 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.20-3.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.25 0 1000 CitHepPh-0.25-0.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.25-0 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.25-0.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.25 1 1000 CitHepPh-0.25-1.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.25-1 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.25-1.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.25 2 1000 CitHepPh-0.25-2.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.25-2 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.25-2.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.cithepph.PRCitHepPhApprox /home/rrosa /home/rrosa 30 0.25 3 1000 CitHepPh-0.25-3.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/CitHepPh-0.25-3 /home/rrosa/Results/PR/CitHepPh-exact 0.99 > /home/rrosa/Eval/PR/CitHepPh-0.25-3.csv

sed -i 's/\./,/g' /home/rrosa/Statistics/CitHepPh/*.csv
