package test.hits;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.link_analysis.HITS;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.algorithm.hits.ApproximatedHITSConfig;
import pt.tecnico.graph.stream.GraphStreamHandler;
import pt.tecnico.graph.stream.GraphUpdateStatistics;
import pt.tecnico.graph.stream.GraphUpdates;

import java.util.Map;

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
    public boolean beforeUpdates(GraphUpdates updates, GraphUpdateStatistics statistics) {
        return true;
    }

    @Override
    public GraphStreamHandler.ObserverResponse onQuery(int id, String query, Graph graph, GraphUpdates updates,
                                                       GraphUpdateStatistics statistics, Map updateInfos,
                                                       ApproximatedHITSConfig config) {
        return GraphStreamHandler.ObserverResponse.COMPUTE_EXACT;
    }

    @Override
    public void onQueryResult(int id, String query, GraphStreamHandler.ObserverResponse response, Graph<Long, NullValue, NullValue> graph,
                              DataSet<Long> computedVertices, DataSet<HITS.Result<Long>> result, JobExecutionResult jobExecutionResult) {
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
