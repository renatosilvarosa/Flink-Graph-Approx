package test;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.algorithm.pagerank.ApproximatedPageRank;
import pt.tecnico.graph.stream.GraphUpdateTracker;

/**
 * Created by Renato on 26/07/2016.
 */
class ExactPRStatistics extends PRStatistics {

    ExactPRStatistics(String dir, String fileName) {
        super(fileName, dir);
    }

    @Override
    public void onStart() throws Exception {
        super.onStart();
        printStream.println("iteration;nVertices;nEdges;time");
    }

    @Override
    public ApproximatedPageRank.DeciderResponse onQuery(int id, String query, Graph<Long, NullValue, NullValue> graph, GraphUpdateTracker<Long, NullValue, NullValue> updateTracker) {
        return ApproximatedPageRank.DeciderResponse.COMPUTE_EXACT;
    }

    @Override
    public void onQueryResult(int id, String query, ApproximatedPageRank.DeciderResponse response, Graph<Long,
            NullValue, NullValue> graph, Graph<Long, Double, Double> summaryGraph, DataSet<Tuple2<Long, Double>> result,
                              JobExecutionResult jobExecutionResult) {
        try {
            long nVertices = graph.numberOfVertices();
            long nEdges = graph.numberOfEdges();

            printStream.format("%d;%d;%d;%d%n", id, nVertices, nEdges, jobExecutionResult.getNetRuntime());
            printStream.flush();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
