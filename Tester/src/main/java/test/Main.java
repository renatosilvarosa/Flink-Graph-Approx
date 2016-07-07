package test;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.job.pagerank.ApproximatedPageRank;
import pt.tecnico.graph.job.pagerank.ApproximatedPageRankConfig;
import pt.tecnico.graph.stream.SocketStreamProvider;

/**
 * Created by Renato on 09/04/2016.
 */
public class Main {
    public static void main(String[] args) {
        //ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", 6123,
                "flink-graph-approx-0.1.jar", "flink-graph-algorithms-0.1.jar"
        );
        env.getConfig().disableSysoutLogging().setParallelism(1);
        try {
            Graph<Long, NullValue, NullValue> graph = Graph.fromCsvReader("D:/Documents/Dissertação/Datasets/Cit-HepTh/Cit-HepTh-init.txt", env)
                    .ignoreCommentsEdges("#")
                    .fieldDelimiterEdges("\t")
                    .keyType(Long.class);

            ApproximatedPageRankConfig config = new ApproximatedPageRankConfig().setBeta(0.85).setIterations(30)
                    .setPrintRanks(false).setNeighborhoodSize(0).setOutputSize(100);

            ApproximatedPageRank approximatedPageRank = new ApproximatedPageRank(new SocketStreamProvider<>("localhost", 1234, s -> s),
                    graph);
            approximatedPageRank.setConfig(config);
            approximatedPageRank.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
