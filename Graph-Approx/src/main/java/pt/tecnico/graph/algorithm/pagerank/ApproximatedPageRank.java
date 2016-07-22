package pt.tecnico.graph.algorithm.pagerank;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import pt.tecnico.graph.algorithm.SimplePageRank;
import pt.tecnico.graph.algorithm.SummarizedGraphPageRank;
import pt.tecnico.graph.stream.GraphStreamHandler;
import pt.tecnico.graph.stream.GraphUpdateStatistics;
import pt.tecnico.graph.stream.GraphUpdateTracker;
import pt.tecnico.graph.stream.StreamProvider;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Renato on 26/06/2016.
 */
public class ApproximatedPageRank extends GraphStreamHandler<Tuple2<Long, Double>> {
    private GraphUpdateTracker<Long> graphUpdateTracker;
    private ApproximatedPageRankConfig config;

    private Set<Edge<Long, NullValue>> edgesToAdd = new HashSet<>();
    private Set<Edge<Long, NullValue>> edgesToRemove = new HashSet<>();

    private TypeSerializerInputFormat<Tuple2<Long, Long>> edgeInputFormat;
    private TypeSerializerOutputFormat<Tuple2<Long, Long>> edgeOutputFormat;
    private ApproximatedPageRankExecutionStatistics executionStatistics;

    private int iteration = 0;
    private PageRankQueryDecider decider;
    private TypeSerializerInputFormat<Tuple2<Long, Double>> rankInputFormat;
    private TypeSerializerOutputFormat<Tuple2<Long, Double>> rankOutputFormat;
    private TypeInformation<Tuple2<Long, Double>> rankTypeInfo;

    public ApproximatedPageRank(StreamProvider<String> updateStream, Graph<Long, NullValue, NullValue> graph) {
        super(updateStream, graph);
    }

    @Override
    public void init() throws Exception {
        graphUpdateTracker = new GraphUpdateTracker<>(graph);

        TypeInformation<Tuple2<Long, Long>> edgeTypeInfo = graph.getEdgeIds().getType();
        edgeInputFormat = new TypeSerializerInputFormat<>(edgeTypeInfo);

        edgeOutputFormat = new TypeSerializerOutputFormat<>();
        edgeOutputFormat.setInputType(edgeTypeInfo, env.getConfig());
        edgeOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);

        edgeOutputFormat.setOutputFilePath(new Path("./edges" + iteration));
        graph.getEdgeIds().output(edgeOutputFormat);

        env.execute("Original graph reading");

        DataSet<Tuple2<Long, Double>> ranks = computeExact();

        rankTypeInfo = ranks.getType();
        rankInputFormat = new TypeSerializerInputFormat<>(rankTypeInfo);
        rankOutputFormat = new TypeSerializerOutputFormat<>();
        rankOutputFormat.setInputType(rankTypeInfo, env.getConfig());
        rankOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
        rankOutputFormat.setOutputFilePath(new Path("./ranks" + iteration));

        // iteration 0
        ranks.output(rankOutputFormat);
        outputResult("", ranks);
        env.execute("First PageRank calculation");

        executionStatistics = new ApproximatedPageRankExecutionStatistics(graph,
                config.getIterations(), env.getLastJobExecutionResult().getNetRuntime());
        graphUpdateTracker.resetAll();
    }

    @Override
    public void run() {
        while (true) {
            try {
                String update = pendingUpdates.take();
                String[] split = update.split(" ");
                switch (split[0]) {
                    case "A": {
                        // add edge
                        registerEdgeAdd(split);
                        break;
                    }
                    case "D":
                        // delete
                        registerEdgeDelete(split);
                        break;
                    case "Q":
                        iteration++;
                        applyUpdates();

                        String tag = split.length > 1 ? split[1] : "";

                        rankInputFormat.setFilePath("ranks" + ((iteration - 1) % 5));
                        DataSet<Tuple2<Long, Double>> previousRanks = env.createInput(rankInputFormat, rankTypeInfo);

                        DeciderResponse response = decider.onQuery(update, this);

                        rankOutputFormat.setOutputFilePath(new Path("./ranks" + (iteration % 5)));

                        DataSet<Tuple2<Long, Double>> newRanks = null;
                        if (response == DeciderResponse.REPEAT_LAST_ANSWER) {
                            newRanks = previousRanks;
                        } else if (response == DeciderResponse.COMPUTE_APPROXIMATE) {
                            newRanks = computeApproximate(previousRanks);
                        } else if (response == DeciderResponse.COMPUTE_EXACT) {
                            newRanks = computeExact();
                        }

                        assert newRanks != null : "Ranks are null";
                        outputResult(tag, newRanks);
                        newRanks.output(rankOutputFormat);

                        env.execute("Approx PageRank it. " + iteration);
                        executionStatistics = new ApproximatedPageRankExecutionStatistics(graph,
                                config.getIterations(), env.getLastJobExecutionResult().getNetRuntime());
                        break;
                    case "END":
                        return;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private DataSet<Tuple2<Long, Double>> computeExact() throws Exception {
        return graph.run(new SimplePageRank<>(config.getBeta(), 1.0, config.getIterations()));
    }

    private DataSet<Tuple2<Long, Double>> computeApproximate(DataSet<Tuple2<Long, Double>> previousRanks) throws Exception {
        Set<Long> updatedIds = graphUpdateTracker.updatedAboveThresholdVertexIds(config.getUpdatedRatioThreshold(), EdgeDirection.ALL);

        Vertex<Long, Double> bigVertex = new Vertex<>(0L, 0.0);
        Graph<Long, Double, Double> representativeGraph = new RepresentativeGraphBuilder<>(graph, 1.0)
                .representativeGraph(env.fromCollection(updatedIds, TypeInformation.of(Long.class)),
                        previousRanks, config.getNeighborhoodSize(), bigVertex);

        DataSet<Tuple2<Long, Double>> ranks = representativeGraph.run(new SummarizedGraphPageRank(config.getBeta(), config.getIterations(), bigVertex.getId()));

        DataSet<Tuple2<Long, Double>> newRanks = previousRanks.coGroup(ranks)
                .where(0).equalTo(0)
                .with(new CoGroupFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<Long, Double>> previous, Iterable<Tuple2<Long, Double>> newRanks, Collector<Tuple2<Long, Double>> out) throws Exception {
                        Iterator<Tuple2<Long, Double>> prevIt = previous.iterator();
                        Iterator<Tuple2<Long, Double>> newIt = newRanks.iterator();

                        if (newIt.hasNext()) {
                            Tuple2<Long, Double> next = newIt.next();
                            if (!next.f0.equals(bigVertex.getId())) {
                                out.collect(next);
                            }
                        } else if (prevIt.hasNext()) {
                            out.collect(prevIt.next());
                        }
                    }
                });

        graphUpdateTracker.reset(updatedIds);
        return newRanks;
    }

    public GraphUpdateStatistics getGraphUpdateStatistics() {
        return graphUpdateTracker.getUpdateStatistics();
    }

    public ApproximatedPageRankExecutionStatistics getLastExecutionStatistics() {
        return executionStatistics;
    }

    private void outputResult(String date, DataSet<Tuple2<Long, Double>> ranks) {
        outputFormat.setIteration(iteration);
        outputFormat.setTags(date);
        ranks.sortPartition(1, Order.DESCENDING).setParallelism(1).first(config.getOutputSize()).output(outputFormat);
    }

    private void applyUpdates() throws Exception {
        edgeInputFormat.setFilePath("./edges" + ((iteration - 1) % 5));
        graph = Graph.fromTuple2DataSet(env.createInput(edgeInputFormat), env);

        if (!edgesToAdd.isEmpty()) {
            List<Vertex<Long, NullValue>> vertices = edgesToAdd.stream()
                    .flatMap(e -> Stream.of(e.getSource(), e.getTarget()))
                    .map(id -> new Vertex<>(id, NullValue.getInstance()))
                    .distinct()
                    .collect(Collectors.toList());

            graph = graph
                    .addVertices(vertices)
                    .addEdges(new ArrayList<>(edgesToAdd));
        }

        if (!edgesToRemove.isEmpty()) {
            graph = graph.removeEdges(new ArrayList<>(edgesToRemove));
        }

        edgeOutputFormat.setOutputFilePath(new Path("./edges" + (iteration % 5)));
        graph.getEdgeIds().output(edgeOutputFormat);
        env.execute("Apply updates it. " + iteration);

        edgesToAdd.clear();
        edgesToRemove.clear();
    }

    private void registerEdgeDelete(String[] split) {
        Edge<Long, NullValue> edge = parseEdge(split);
        edgesToRemove.add(edge);
        graphUpdateTracker.removeEdge(edge.getSource(), edge.getTarget());
    }

    private void registerEdgeAdd(String[] split) {
        Vertex<Long, NullValue>[] vertices = parseVertices(split);
        Edge<Long, NullValue> edge = parseEdge(split);
        edgesToAdd.add(edge);
        graphUpdateTracker.addEdge(vertices[0].getId(), vertices[1].getId());
    }

    @SuppressWarnings("unchecked")
    private Vertex<Long, NullValue>[] parseVertices(String[] data) {
        assert data.length == 3;
        Vertex<Long, NullValue> v1 = new Vertex<>(Long.valueOf(data[1]), NullValue.getInstance());
        Vertex<Long, NullValue> v2 = new Vertex<>(Long.valueOf(data[2]), NullValue.getInstance());
        return (Vertex<Long, NullValue>[]) new Vertex[]{v1, v2};
    }

    private Edge<Long, NullValue> parseEdge(String[] data) {
        assert data.length == 3;
        return new Edge<>(Long.valueOf(data[1]), Long.valueOf(data[2]), NullValue.getInstance());
    }

    public ApproximatedPageRankConfig getConfig() {
        return config;
    }

    public void setConfig(ApproximatedPageRankConfig config) {
        this.config = config;
    }

    public ApproximatedPageRank setDecider(PageRankQueryDecider decider) {
        this.decider = decider;
        return this;
    }

    public enum DeciderResponse {
        REPEAT_LAST_ANSWER,
        COMPUTE_APPROXIMATE,
        COMPUTE_EXACT
    }

    @FunctionalInterface
    public interface PageRankQueryDecider {
        DeciderResponse onQuery(String query, ApproximatedPageRank algorithm);
    }
}
