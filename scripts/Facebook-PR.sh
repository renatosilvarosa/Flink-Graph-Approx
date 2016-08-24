#!/bin/bash

mkdir -p /home/rrosa/Eval/PR
java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.facebook.PRFacebookExact /home/rrosa /home/rrosa 20 1000 Facebook-exact.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.facebook.PRFacebookApprox /home/rrosa /home/rrosa 20 0.00 0 1000 Facebook-0.00-0.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/Facebook-0.00-0 /home/rrosa/Results/PR/Facebook-exact 0.99 > /home/rrosa/Eval/PR/Facebook-0.00-0.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.facebook.PRFacebookApprox /home/rrosa /home/rrosa 20 0.00 1 1000 Facebook-0.00-1.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/Facebook-0.00-1 /home/rrosa/Results/PR/Facebook-exact 0.99 > /home/rrosa/Eval/PR/Facebook-0.00-1.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.facebook.PRFacebookApprox /home/rrosa /home/rrosa 20 0.00 2 1000 Facebook-0.00-2.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/Facebook-0.00-2 /home/rrosa/Results/PR/Facebook-exact 0.99 > /home/rrosa/Eval/PR/Facebook-0.00-2.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.facebook.PRFacebookApprox /home/rrosa /home/rrosa 20 0.10 0 1000 Facebook-0.10-0.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/Facebook-0.10-0 /home/rrosa/Results/PR/Facebook-exact 0.99 > /home/rrosa/Eval/PR/Facebook-0.10-0.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.facebook.PRFacebookApprox /home/rrosa /home/rrosa 20 0.10 1 1000 Facebook-0.10-1.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/Facebook-0.10-1 /home/rrosa/Results/PR/Facebook-exact 0.99 > /home/rrosa/Eval/PR/Facebook-0.10-1.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.facebook.PRFacebookApprox /home/rrosa /home/rrosa 20 0.10 2 1000 Facebook-0.10-2.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/Facebook-0.10-2 /home/rrosa/Results/PR/Facebook-exact 0.99 > /home/rrosa/Eval/PR/Facebook-0.10-2.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.facebook.PRFacebookApprox /home/rrosa /home/rrosa 20 0.20 0 1000 Facebook-0.20-0.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/Facebook-0.20-0 /home/rrosa/Results/PR/Facebook-exact 0.99 > /home/rrosa/Eval/PR/Facebook-0.20-0.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.facebook.PRFacebookApprox /home/rrosa /home/rrosa 20 0.20 1 1000 Facebook-0.20-1.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/Facebook-0.20-1 /home/rrosa/Results/PR/Facebook-exact 0.99 > /home/rrosa/Eval/PR/Facebook-0.20-1.csv

java -cp .:flink-graph-tester-0.3.jar:dependency/* test.pr.facebook.PRFacebookApprox /home/rrosa /home/rrosa 20 0.20 2 1000 Facebook-0.20-2.csv
/home/rrosa/Flink-Graph-Approx/python/batch-rank-evaluator.py /home/rrosa/Results/PR/Facebook-0.20-2 /home/rrosa/Results/PR/Facebook-exact 0.99 > /home/rrosa/Eval/PR/Facebook-0.20-2.csv

sed -i 's/\./,/g' /home/rrosa/Statistics/Facebook/*.csv