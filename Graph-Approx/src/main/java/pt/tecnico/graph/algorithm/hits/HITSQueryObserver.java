package pt.tecnico.graph.algorithm.hits;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.link_analysis.HITS;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.stream.GraphStreamHandler;
import pt.tecnico.graph.stream.GraphUpdateStatistics;
import pt.tecnico.graph.stream.GraphUpdateTracker;
import pt.tecnico.graph.stream.GraphUpdates;

import java.util.Map;

public interface HITSQueryObserver<K, EV> {
    void onStart() throws Exception;

    boolean beforeUpdates(GraphUpdates<K, EV> updates, GraphUpdateStatistics statistics);

    GraphStreamHandler.ObserverResponse onQuery(int id, String query, Graph<Long, NullValue, NullValue> graph,
                                                GraphUpdates<K, EV> updates, GraphUpdateStatistics statistics,
                                                Map<K, GraphUpdateTracker.UpdateInfo> updateInfos,
                                                ApproximatedHITSConfig config);

    void onQueryResult(int id, String query, GraphStreamHandler.ObserverResponse response, Graph<Long, NullValue, NullValue> graph,
                       DataSet<Long> computedVertices, DataSet<HITS.Result<Long>> result,
                       JobExecutionResult jobExecutionResult);

    void onStop() throws Exception;
}
