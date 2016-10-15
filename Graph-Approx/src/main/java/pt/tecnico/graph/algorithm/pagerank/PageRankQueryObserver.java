package pt.tecnico.graph.algorithm.pagerank;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.stream.GraphStreamHandler;
import pt.tecnico.graph.stream.GraphUpdateStatistics;
import pt.tecnico.graph.stream.GraphUpdateTracker;
import pt.tecnico.graph.stream.GraphUpdates;

import java.util.Map;

public interface PageRankQueryObserver<K, EV> {
    void onStart() throws Exception;

    boolean beforeUpdates(GraphUpdates<K, EV> updates, GraphUpdateStatistics statistics);

    GraphStreamHandler.Action onQuery(int id, String query, Graph<Long, NullValue, NullValue> graph,
                                      GraphUpdates<K, EV> updates, GraphUpdateStatistics statistics,
                                      Map<K, GraphUpdateTracker.UpdateInfo> updateInfos,
                                      ApproximatePageRankConfig config);

    void onQueryResult(int id, String query, GraphStreamHandler.Action action, Graph<Long, NullValue, NullValue> graph,
                       Graph<Long, Double, Double> summaryGraph, DataSet<Tuple2<Long, Double>> result,
                       JobExecutionResult jobExecutionResult);

    void onStop() throws Exception;
}
