package test;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.algorithm.pagerank.ApproximatedPageRank;
import pt.tecnico.graph.algorithm.pagerank.ApproximatedPageRankConfig;
import pt.tecnico.graph.algorithm.pagerank.PageRankCsvOutputFormat;
import pt.tecnico.graph.stream.SocketStreamProvider;

import java.time.LocalDateTime;

/**
 * Created by Renato on 09/04/2016.
 */
public class CitHepPh {
    public static void main(String[] args) {
        //ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", 6123,
                "flink-graph-approx-0.1.jar", "flink-graph-algorithms-0.1.jar"
        );
        env.getConfig().disableSysoutLogging().setParallelism(1);
        try {
            Graph<Long, NullValue, NullValue> graph = Graph.fromCsvReader("D:/Documents/Dissertação/Datasets/Cit-HepPh/Cit-HepPh-init.txt", env)
                    .ignoreCommentsEdges("#")
                    .fieldDelimiterEdges("\t")
                    .keyType(Long.class);

            ApproximatedPageRankConfig config = new ApproximatedPageRankConfig().setBeta(0.85).setIterations(20)
                    .setNeighborhoodSize(1).setOutputSize(1000);

            LocalDateTime.now();
            PageRankCsvOutputFormat outputFormat = new PageRankCsvOutputFormat("D:/Documents/Dissertação/Results/", System.lineSeparator(), ";", false, true);
            outputFormat.setName("pageRank");

            ApproximatedPageRank approximatedPageRank = new ApproximatedPageRank(new SocketStreamProvider<>("localhost", 1234, s -> s),
                    graph);
            approximatedPageRank.setConfig(config);
            approximatedPageRank.setOutputFormat(outputFormat);

            approximatedPageRank.setDecider((query, algorithm) -> {
/*                ApproximatedPageRankExecutionStatistics lastExecutionStatistics = algorithm.getLastExecutionStatistics();
                try {
                    System.err.println("\tSummary vertices: " + lastExecutionStatistics.getNumberOfSummaryVertices());
                    System.err.println("\tSummary edges: " + lastExecutionStatistics.getNumberOfSummaryEdges());
                    System.err.println("\tPR iterations: " + lastExecutionStatistics.getNumberOfIterations());
                    System.err.println("\tExecution time: " + lastExecutionStatistics.getExecutionTime());
                    System.err.println();
                    System.err.println();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                System.err.println(query);

                GraphUpdateStatistics updateStatistics = algorithm.getGraphUpdateStatistics();
                System.err.println("\tAdded vertices: " + updateStatistics.getAddedVertices());
                System.err.println("\tRemoved vertices: " + updateStatistics.getRemovedVertices());
                System.err.println("\tAdded edges: " + updateStatistics.getAddedEdges());
                System.err.println("\tRemoved vertices: " + updateStatistics.getRemovedEdges());
                System.err.println("\tUpdated vertices: " + updateStatistics.getUpdatedVertices());
                System.err.println("\tTotal vertices: " + updateStatistics.getTotalVertices());
                System.err.println("\tTotal edges: " + updateStatistics.getTotalEdges());*/

                return ApproximatedPageRank.DeciderResponse.COMPUTE_APPROXIMATE;

            });

            approximatedPageRank.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
