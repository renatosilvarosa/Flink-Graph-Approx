package test;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.algorithm.SimplePageRank;

/**
 * Created by Renato on 30/03/2016.
 */
public class Test {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", 6123,
                "flink-graph-algorithms-0.1.jar", "flink-graph-approx-0.1.jar", "flink-graph-tester-0.1.jar");

        Graph<Long, NullValue, NullValue> graph = Graph.fromCsvReader("D:/Documents/Dissertação/Datasets/Cit-HepPh/Cit-HepPh-init.txt", env)
                .ignoreCommentsEdges("#")
                .fieldDelimiterEdges("\t")
                .keyType(Long.class);

        try {


            DataSet<Tuple2<Long, Double>> result1 = graph.run(new SimplePageRank<>(0.85, 1.0, 30));
            result1.sortPartition(1, Order.DESCENDING).writeAsCsv("D:/Documents/Dissertação/pageRank1.csv", FileSystem.WriteMode.OVERWRITE);
            env.execute();
            System.err.println(env.getLastJobExecutionResult().getNetRuntime());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
