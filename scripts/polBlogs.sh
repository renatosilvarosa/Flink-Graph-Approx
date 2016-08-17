#! /bin/bash

java -cp .:flink-graph-tester-0.1.jar:dependency/* test.PolBlogsExact /home/rrosa /home/rrosa 30 1000 exact-pr.csv

java -cp .:flink-graph-tester-0.1.jar:dependency/* test.PolBlogs /home/rrosa /home/rrosa 30 0.00 0 1000 approx-t0-n0.csv
/home/rrosa/python/page-rank-evaluator.py /home/rrosa/Results/PolBlogs-0.00-00 /home/rrosa/Results/PolBlogs-exact 0.99 /home/rrosa/Statistics/polblogs

java -cp .:flink-graph-tester-0.1.jar:dependency/* test.PolBlogs /home/rrosa /home/rrosa 30 0.00 1 1000 approx-t0-n1.csv
/home/rrosa/python/page-rank-evaluator.py /home/rrosa/Results/PolBlogs-0.00-01 /home/rrosa/Results/PolBlogs-exact 0.99 /home/rrosa/Statistics/polblogs

java -cp .:flink-graph-tester-0.1.jar:dependency/* test.PolBlogs /home/rrosa /home/rrosa 30 0.00 2 1000 approx-t0-n2.csv
/home/rrosa/python/page-rank-evaluator.py /home/rrosa/Results/PolBlogs-0.00-02 /home/rrosa/Results/PolBlogs-exact 0.99 /home/rrosa/Statistics/polblogs

java -cp .:flink-graph-tester-0.1.jar:dependency/* test.PolBlogs /home/rrosa /home/rrosa 30 0.05 0 1000 approx-t005-n0.csv
/home/rrosa/python/page-rank-evaluator.py /home/rrosa/Results/PolBlogs-0.05-00 /home/rrosa/Results/PolBlogs-exact 0.99 /home/rrosa/Statistics/polblogs

java -cp .:flink-graph-tester-0.1.jar:dependency/* test.PolBlogs /home/rrosa /home/rrosa 30 0.05 1 1000 approx-t005-n1.csv
/home/rrosa/python/page-rank-evaluator.py /home/rrosa/Results/PolBlogs-0.05-01 /home/rrosa/Results/PolBlogs-exact 0.99 /home/rrosa/Statistics/polblogs

java -cp .:flink-graph-tester-0.1.jar:dependency/* test.PolBlogs /home/rrosa /home/rrosa 30 0.05 2 1000 approx-t005-n2.csv
/home/rrosa/python/page-rank-evaluator.py /home/rrosa/Results/PolBlogs-0.05-02 /home/rrosa/Results/PolBlogs-exact 0.99 /home/rrosa/Statistics/polblogs

java -cp .:flink-graph-tester-0.1.jar:dependency/* test.PolBlogs /home/rrosa /home/rrosa 30 0.1 0 1000 approx-t01-n0.csv
/home/rrosa/python/page-rank-evaluator.py /home/rrosa/Results/PolBlogs-0.10-00 /home/rrosa/Results/PolBlogs-exact 0.99 /home/rrosa/Statistics/polblogs

java -cp .:flink-graph-tester-0.1.jar:dependency/* test.PolBlogs /home/rrosa /home/rrosa 30 0.1 1 1000 approx-t01-n1.csv
/home/rrosa/python/page-rank-evaluator.py /home/rrosa/Results/PolBlogs-0.10-01 /home/rrosa/Results/PolBlogs-exact 0.99 /home/rrosa/Statistics/polblogs

java -cp .:flink-graph-tester-0.1.jar:dependency/* test.PolBlogs /home/rrosa /home/rrosa 30 0.1 2 1000 approx-t01-n2.csv
/home/rrosa/python/page-rank-evaluator.py /home/rrosa/Results/PolBlogs-0.10-02 /home/rrosa/Results/PolBlogs-exact 0.99 /home/rrosa/Statistics/polblogs

java -cp .:flink-graph-tester-0.1.jar:dependency/* test.PolBlogs /home/rrosa /home/rrosa 30 0.15 0 1000 approx-t015-n0.csv
/home/rrosa/python/page-rank-evaluator.py /home/rrosa/Results/PolBlogs-0.15-00 /home/rrosa/Results/PolBlogs-exact 0.99 /home/rrosa/Statistics/polblogs

java -cp .:flink-graph-tester-0.1.jar:dependency/* test.PolBlogs /home/rrosa /home/rrosa 30 0.15 1 1000 approx-t015-n1.csv
/home/rrosa/python/page-rank-evaluator.py /home/rrosa/Results/PolBlogs-0.15-01 /home/rrosa/Results/PolBlogs-exact 0.99 /home/rrosa/Statistics/polblogs

java -cp .:flink-graph-tester-0.1.jar:dependency/* test.PolBlogs /home/rrosa /home/rrosa 30 0.15 2 1000 approx-t015-n2.csv
/home/rrosa/python/page-rank-evaluator.py /home/rrosa/Results/PolBlogs-0.15-02 /home/rrosa/Results/PolBlogs-exact 0.99 /home/rrosa/Statistics/polblogs

sed -i 's/\./,/g' /home/rrosa/Statistics/polblogs/*.csv