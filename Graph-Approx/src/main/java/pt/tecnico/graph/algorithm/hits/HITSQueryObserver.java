package pt.tecnico.graph.algorithm.hits;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.link_analysis.HITS;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.stream.GraphUpdateTracker;

/**
 * Created by Renato on 26/07/2016.
 */
public interface HITSQueryObserver {
    void onStart() throws Exception;

    ApproximatedHITS.DeciderResponse onQuery(int id, String query, Graph<Long, NullValue, NullValue> graph, GraphUpdateTracker<Long, NullValue, NullValue> updateTracker);

    void onQueryResult(int id, String query, ApproximatedHITS.DeciderResponse response, Graph<Long, NullValue, NullValue> graph,
                       DataSet<Long> computedVertices, DataSet<HITS.Result<Long>> result,
                       JobExecutionResult jobExecutionResult);

    void onStop() throws Exception;
}
