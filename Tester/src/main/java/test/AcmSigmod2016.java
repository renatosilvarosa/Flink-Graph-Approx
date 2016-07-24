package test;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.algorithm.pagerank.ApproximatedPageRank;
import pt.tecnico.graph.algorithm.pagerank.ApproximatedPageRankConfig;
import pt.tecnico.graph.algorithm.pagerank.PageRankCsvOutputFormat;
import pt.tecnico.graph.stream.SocketStreamProvider;

/**
 * Created by Renato on 09/04/2016.
 */
public class AcmSigmod2016 {
    public static void main(String[] args) {
        //ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("146.193.41.145", 6123,
                "flink-graph-approx-0.1.jar", "flink-graph-algorithms-0.1.jar"
        );
        env.getConfig()
                //.disableSysoutLogging()
                .setParallelism(1);
        try {
            Graph<Long, NullValue, NullValue> graph = Graph.fromCsvReader("D:/Documents/Dissertação/Datasets/test-harness/init-file.txt", env)
                    .ignoreCommentsEdges("#")
                    .fieldDelimiterEdges("\t")
                    .keyType(Long.class);

            ApproximatedPageRankConfig config = new ApproximatedPageRankConfig().setBeta(0.85).setIterations(20)
                    .setNeighborhoodSize(1).setOutputSize(1000);

            PageRankCsvOutputFormat outputFormat = new PageRankCsvOutputFormat("~/Results/AcmSigmod2016/", System.lineSeparator(), ";", false, true);
            outputFormat.setName("pageRank");

            ApproximatedPageRank approximatedPageRank = new ApproximatedPageRank(new SocketStreamProvider<>("146.193.41.145", 5678, s -> s),
                    graph);
            approximatedPageRank.setConfig(config);
            approximatedPageRank.setOutputFormat(outputFormat);

            approximatedPageRank.setDecider((query, algorithm) -> ApproximatedPageRank.DeciderResponse.COMPUTE_APPROXIMATE);

            approximatedPageRank.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}