package test.hits;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.link_analysis.HITS;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.algorithm.hits.ApproximatedHITS;
import pt.tecnico.graph.stream.GraphUpdateTracker;

/**
 * Created by Renato on 26/07/2016.
 */
public class ApproximatedHITSStatistics extends HITSStatistics {

    public ApproximatedHITSStatistics(String dir, String fileName) {
        super(fileName, dir);
    }

    @Override
    public void onStart() throws Exception {
        super.onStart();
        printStream.println("iteration;nVertices;nEdges;time");
    }

    @Override
    public ApproximatedHITS.DeciderResponse onQuery(int id, String query, Graph<Long, NullValue, NullValue> graph, GraphUpdateTracker<Long, NullValue, NullValue> updateTracker) {
        return ApproximatedHITS.DeciderResponse.COMPUTE_APPROXIMATE;
    }

    @Override
    public void onQueryResult(int id, String query, ApproximatedHITS.DeciderResponse response, Graph<Long, NullValue, NullValue> graph,
                              DataSet<Long> computedVertices, DataSet<HITS.Result<Long>> result, JobExecutionResult jobExecutionResult) {
        try {
            long nVertices = graph.numberOfVertices();
            long nEdges = graph.numberOfEdges();

            printStream.format("%d;%d;%d;%d;%d%n", id, nVertices, nEdges, computedVertices.count(), jobExecutionResult.getNetRuntime());
            printStream.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
