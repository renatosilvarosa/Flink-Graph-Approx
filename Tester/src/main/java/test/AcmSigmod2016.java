package test;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.algorithm.pagerank.ApproximatedPageRank;
import pt.tecnico.graph.algorithm.pagerank.ApproximatedPageRankConfig;
import pt.tecnico.graph.algorithm.pagerank.PageRankCsvOutputFormat;
import pt.tecnico.graph.algorithm.pagerank.PageRankQueryObserver;
import pt.tecnico.graph.stream.GraphUpdateTracker;
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

            ApproximatedPageRank approximatedPageRank = new ApproximatedPageRank(new SocketStreamProvider("146.193.41.145", 5678),
                    graph);
            approximatedPageRank.setConfig(config);
            approximatedPageRank.setOutputFormat(outputFormat);

            approximatedPageRank.setObserver(new PageRankQueryObserver() {
                @Override
                public void onStart() {

                }

                @Override
                public ApproximatedPageRank.DeciderResponse onQuery(int id, String query, Graph<Long, NullValue,
                        NullValue> graph, GraphUpdateTracker<Long, NullValue, NullValue> updateTracker) {
                    return ApproximatedPageRank.DeciderResponse.COMPUTE_APPROXIMATE;
                }

                @Override
                public void onQueryResult(int id, String query, ApproximatedPageRank.DeciderResponse response, Graph<Long, NullValue, NullValue> graph, Graph<Long, Double, Double> summaryGraph, DataSet<Tuple2<Long, Double>> result, JobExecutionResult jobExecutionResult) {

                }

                @Override
                public void onStop() {

                }
            });

            approximatedPageRank.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
