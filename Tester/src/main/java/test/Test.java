package test;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.link_analysis.HITS;
import org.apache.flink.types.NullValue;

/**
 * Created by Renato on 30/03/2016.
 */
public class Test {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("146.193.41.145", 6123,
                "flink-graph-approx-0.2.jar", "flink-graph-algorithms-0.2.jar");

        Graph<Long, NullValue, NullValue> graph = Graph.fromCsvReader("/home/rrosa/Datasets/Cit-HepPh/Cit-HepPh-init.txt", env)
                .ignoreCommentsEdges("#")
                .fieldDelimiterEdges("\t")
                .keyType(Long.class);

        try {
            DataSet<HITS.Result<Long>> result = graph.run(new HITS<>(30));
            result.sortPartition("f1.f0", Order.DESCENDING).setParallelism(1).writeAsCsv("/home/rrosa/hubs.csv", FileSystem.WriteMode.OVERWRITE);
            result.sortPartition("f1.f1", Order.DESCENDING).setParallelism(1).writeAsCsv("/home/rrosa/authorities.csv", FileSystem.WriteMode.OVERWRITE);
            env.execute();
            System.err.println(env.getLastJobExecutionResult().getNetRuntime());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
