package pt.tecnico.graph.algorithm.pagerank;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.stream.GraphUpdateTracker;

/**
 * Created by Renato on 26/07/2016.
 */
public interface PageRankQueryObserver {
    void onStart() throws Exception;

    ApproximatedPageRank.DeciderResponse onQuery(int id, String query, Graph<Long, NullValue, NullValue> graph, GraphUpdateTracker<Long, NullValue, NullValue> updateTracker);

    void onQueryResult(int id, String query, ApproximatedPageRank.DeciderResponse response, Graph<Long, NullValue, NullValue> graph,
                       Graph<Long, Double, Double> summaryGraph, DataSet<Tuple2<Long, Double>> result,
                       JobExecutionResult jobExecutionResult);

    void onStop() throws Exception;
}
