package test.hits;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.link_analysis.HITS;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.algorithm.hits.ApproximatedHITS;
import pt.tecnico.graph.stream.GraphUpdateTracker;

/**
 * Created by Renato on 26/07/2016.
 */
public class ExactHITSStatistics extends HITSStatistics {

    public ExactHITSStatistics(String dir, String fileName) {
        super(fileName, dir);
    }

    @Override
    public void onStart() throws Exception {
        super.onStart();
        printStream.println("iteration;nVertices;nEdges;time");
    }

    @Override
    public ApproximatedHITS.DeciderResponse onQuery(int id, String query, Graph<Long, NullValue, NullValue> graph, GraphUpdateTracker<Long, NullValue, NullValue> updateTracker) {
        return ApproximatedHITS.DeciderResponse.COMPUTE_EXACT;
    }

    @Override
    public void onQueryResult(int id, String query, ApproximatedHITS.DeciderResponse response, Graph<Long, NullValue, NullValue> graph,
                              DataSet<Long> computedVertices, DataSet<HITS.Result<Long>> result, JobExecutionResult jobExecutionResult) {
        try {
            long nVertices = graph.numberOfVertices();
            long nEdges = graph.numberOfEdges();

            printStream.format("%d;%d;%d;%d%n", id, nVertices, nEdges, jobExecutionResult.getNetRuntime());
            printStream.flush();

            result.sortPartition("f1.f0", Order.DESCENDING).project(1).project(0).writeAsCsv(String.format("%s/result-hub-%02d.csv", dir, id),
                    System.lineSeparator(), ";", FileSystem.WriteMode.OVERWRITE);

            result.sortPartition("f1.f1", Order.DESCENDING).project(1).project(0).writeAsCsv(String.format("%s/result-auth-%02d.csv", dir, id),
                    System.lineSeparator(), ";", FileSystem.WriteMode.OVERWRITE);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
