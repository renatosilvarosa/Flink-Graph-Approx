package test;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import pt.tecnico.graph.algorithm.pagerank.ApproximatedPageRank;
import pt.tecnico.graph.algorithm.pagerank.PageRankQueryObserver;
import pt.tecnico.graph.stream.GraphUpdateTracker;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by Renato on 26/07/2016.
 */
class ExactPRStatistics implements PageRankQueryObserver {

    private final String dir;
    private PrintStream printStream;

    ExactPRStatistics(String dir) {
        this.dir = dir;
    }


    @Override
    public void onStart() {
        try {
            Path dirs = Files.createDirectories(Paths.get(dir));

            Path file = dirs.resolve("exact-pr-time.csv");
            if (!Files.exists(file)) {
                file = Files.createFile(file);
            }
            printStream = new PrintStream(file.toString());
            printStream.println("iteration;nVertices;nEdges;time");
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    @Override
    public void onStop() {
        printStream.close();
    }
}
