package test.pr;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.algorithm.pagerank.ApproximatePageRankConfig;
import pt.tecnico.graph.stream.GraphStreamHandler;
import pt.tecnico.graph.stream.GraphUpdateStatistics;
import pt.tecnico.graph.stream.GraphUpdateTracker;
import pt.tecnico.graph.stream.GraphUpdates;

import java.util.Map;

public class ApproximatedPRStatistics extends PRStatistics {

    public ApproximatedPRStatistics(String dir, String fileName) {
        super(fileName, dir);
    }

    @Override
    public void onStart() throws Exception {
        super.onStart();
        printStream.println("iteration;nVertices;nEdges;nSummVertices;nSummEdges;time");
    }

    @Override
    public boolean beforeUpdates(GraphUpdates<Long, NullValue> updates, GraphUpdateStatistics statistics) {
        return true;
    }

    @Override
    public GraphStreamHandler.Action onQuery(int id, String query, Graph<Long, NullValue, NullValue> graph,
                                             GraphUpdates<Long, NullValue> updates, GraphUpdateStatistics statistics,
                                             Map<Long, GraphUpdateTracker.UpdateInfo> updateInfos,
                                             ApproximatePageRankConfig config) {
        return GraphStreamHandler.Action.COMPUTE_APPROXIMATE;
    }

    @Override
    public void onQueryResult(int id, String query, GraphStreamHandler.Action action, Graph<Long,
            NullValue, NullValue> graph, Graph<Long, Double, Double> summaryGraph, DataSet<Tuple2<Long, Double>> result,
                              JobExecutionResult jobExecutionResult) {
        try {
            long nVertices = graph.numberOfVertices();
            long nEdges = graph.numberOfEdges();
            long summVertices = summaryGraph.numberOfVertices();
            long summEdges = summaryGraph.numberOfEdges();

            printStream.format("%d;%d;%d;%d;%d;%d%n", id, nVertices, nEdges, summVertices, summEdges, jobExecutionResult.getNetRuntime());
            printStream.flush();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
