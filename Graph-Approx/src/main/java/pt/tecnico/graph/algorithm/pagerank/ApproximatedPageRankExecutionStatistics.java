package pt.tecnico.graph.algorithm.pagerank;

import org.apache.flink.graph.Graph;

/**
 * Created by Renato on 12/07/2016.
 */
public class ApproximatedPageRankExecutionStatistics {
    private final Graph<?, ?, ?> summaryGraph;
    private Long numberOfSummaryVertices;
    private Long numberOfSummaryEdges;
    private int numberOfIterations;
    private long executionTime;

    public ApproximatedPageRankExecutionStatistics(Graph<?, ?, ?> summaryGraph, int numberOfIterations, long executionTime) {
        this.summaryGraph = summaryGraph;
        this.numberOfIterations = numberOfIterations;
        this.executionTime = executionTime;
    }

    public long getNumberOfSummaryVertices() throws Exception {
        if (numberOfSummaryVertices == null) {
            numberOfSummaryVertices = summaryGraph.numberOfVertices();
        }
        return numberOfSummaryVertices;
    }

    public long getNumberOfSummaryEdges() throws Exception {
        if (numberOfSummaryEdges == null) {
            numberOfSummaryEdges = summaryGraph.numberOfEdges();
        }
        return numberOfSummaryEdges;
    }

    public int getNumberOfIterations() {
        return numberOfIterations;
    }

    public long getExecutionTime() {
        return executionTime;
    }
}
